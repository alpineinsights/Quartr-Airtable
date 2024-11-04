import streamlit as st
import boto3
import requests
import json
from datetime import datetime
import pandas as pd
import asyncio
import aiohttp
import aioboto3
from typing import List, Dict, Any
import io
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from pyairtable import Api, Base, Table

# Page configuration
st.set_page_config(
    page_title="Quartr Data Retrieval",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = []

class QuartrAPI:
    def __init__(self):
        self.api_key = st.secrets["quartr"]["API_KEY"]
        self.base_url = "https://api.quartr.com/public/v1"
        self.headers = {"X-Api-Key": self.api_key}
        
    async def get_company_events(self, isin: str, session: aiohttp.ClientSession) -> Dict:
        url = f"{self.base_url}/companies/isin/{isin}"
        try:
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    st.error(f"Error fetching data for ISIN {isin}: {response.status}")
                    return {}
        except Exception as e:
            st.error(f"Error fetching data for ISIN {isin}: {str(e)}")
            return {}

class AirtableHandler:
    def __init__(self):
        self.api_key = st.secrets["airtable"]["AIRTABLE_API_KEY"]
        self.base_id = st.secrets["airtable"]["AIRTABLE_BASE_ID"]
        self.table_name = st.secrets["airtable"]["AIRTABLE_TABLE_NAME"]
        self.api = Api(self.api_key)
        self.table = self.api.table(self.base_id, self.table_name)

    async def create_record(self, 
                          company: str,
                          isin: str,
                          aws_url: str,
                          event_date: str,
                          event_type: str,
                          document_type: str) -> bool:
        try:
            # Format the date to Airtable's preferred format
            formatted_date = datetime.strptime(event_date.split('T')[0], '%Y-%m-%d').strftime('%Y-%m-%d')
            
            # Create the record
            record = {
                "Company": company,
                "ISIN": isin,
                "AWS URL": aws_url,
                "Event Date": formatted_date,
                "Event Type": event_type,
                "Document Type": document_type
            }
            
            self.table.create(record)
            return True
        except Exception as e:
            st.error(f"Error creating Airtable record: {str(e)}")
            return False

class TranscriptProcessor:
    @staticmethod
    async def process_transcript(transcript_url: str, transcripts: Dict, session: aiohttp.ClientSession) -> str:
        """Process transcript JSON into clean text"""
        try:
            raw_transcript_url = transcripts.get('transcriptUrl')
            if not raw_transcript_url:
                st.warning(f"No raw transcript URL found in transcripts object")
                return ''

            async with session.get(raw_transcript_url) as response:
                if response.status == 200:
                    if 'application/json' in response.headers.get('Content-Type', ''):
                        try:
                            transcript_data = await response.json()
                            text = transcript_data.get('transcript', {}).get('text', '')
                            sentences = text.split('. ')
                            formatted_text = '.\n'.join(sentences)
                            return formatted_text
                        except json.JSONDecodeError:
                            st.error(f"Error decoding transcript JSON from {raw_transcript_url}")
                            return ''
                    else:
                        st.warning(f"Unexpected content type for transcript: {response.headers.get('Content-Type')}")
                        return ''
                else:
                    st.warning(f"Failed to fetch transcript: {response.status}")
                    return ''
        except Exception as e:
            st.warning(f"Error processing transcript: {str(e)}")
            return ''

    @staticmethod
    def create_pdf(company_name: str, event_title: str, event_date: str, transcript_text: str) -> bytes:
        """Create a PDF from transcript text"""
        buffer = io.BytesIO()
        
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=72
        )

        styles = getSampleStyleSheet()
        
        header_style = ParagraphStyle(
            'CustomHeader',
            parent=styles['Heading1'],
            fontSize=14,
            spaceAfter=30,
            textColor=colors.HexColor('#1a472a'),
            alignment=1
        )
        
        text_style = ParagraphStyle(
            'CustomText',
            parent=styles['Normal'],
            fontSize=10,
            leading=14,
            spaceBefore=6,
            fontName='Helvetica'
        )

        story = []
        
        header_text = f"""
            <para alignment="center">
            <b>{company_name}</b><br/>
            <br/>
            Event: {event_title}<br/>
            Date: {event_date}
            </para>
        """
        story.append(Paragraph(header_text, header_style))
        story.append(Spacer(1, 30))

        paragraphs = transcript_text.split('\n')
        for para in paragraphs:
            if para.strip():
                story.append(Paragraph(para, text_style))
                story.append(Spacer(1, 6))

        doc.build(story)
        return buffer.getvalue()

class S3Handler:
    def __init__(self):
        self.session = aioboto3.Session(
            aws_access_key_id=st.secrets["aws"]["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["aws"]["AWS_SECRET_ACCESS_KEY"],
            region_name=st.secrets["aws"]["AWS_DEFAULT_REGION"]
        )

    async def upload_file(self, file_data: bytes, s3_key: str, bucket_name: str, 
                         content_type: str = 'application/pdf'):
        try:
            async with self.session.client('s3') as s3:
                await s3.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=file_data,
                    ContentType=content_type
                )
                return True
        except Exception as e:
            st.error(f"Error uploading to S3: {str(e)}")
            return False

def format_s3_key(company_name: str, date: str, doc_type: str, filename: str) -> str:
    """Format S3 key with proper naming convention"""
    clean_company = company_name.replace(" ", "_").replace("/", "_").lower()
    clean_date = date.split("T")[0]
    clean_filename = filename.replace(" ", "_").lower()
    return f"{clean_company}/{clean_date}/{doc_type}/{clean_filename}"

async def process_documents(isin_list: List[str], start_date: str, end_date: str, 
                          selected_docs: List[str], bucket_name: str):
    quartr = QuartrAPI()
    s3_handler = S3Handler()
    transcript_processor = TranscriptProcessor()
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    files_processed = st.empty()
    
    total_files = 0
    processed_files = 0
    successful_uploads = 0
    failed_uploads = 0
    
    try:
        async with aiohttp.ClientSession() as session:
            valid_isins = []
            for isin in isin_list:
                company_data = await quartr.get_company_events(isin, session)
                if company_data and 'events' in company_data:
                    valid_isins.append(isin)
                else:
                    st.warning(f"Skipping invalid ISIN {isin}")
            
            if not valid_isins:
                st.error("No valid ISINs found")
                return
                
            companies_data = []
            for isin in valid_isins:
                data = await quartr.get_company_events(isin, session)
                if data:
                    companies_data.append(data)
            
            for company in companies_data:
                for event in company.get('events', []):
                    event_date = event.get('eventDate', '').split('T')[0]
                    if start_date <= event_date <= end_date:
                        for doc_type in selected_docs:
                            url_field = f'{doc_type}Url'
                            if event.get(url_field):
                                total_files += 1
            
            if total_files == 0:
                st.warning("No matching documents found for the specified criteria.")
                return

# Process files
            for company in companies_data:
                if not company:
                    continue
                    
                company_name = company.get('displayName', 'unknown')
                current_isin = company.get('isins', ['unknown'])[0]  # Get the first ISIN
                
                for event in company.get('events', []):
                    event_date = event.get('eventDate', '').split('T')[0]
                    event_title = event.get('eventTitle', 'Unknown Event')
                    
                    if start_date <= event_date <= end_date:
                        for doc_type in selected_docs:
                            url_field = f'{doc_type}Url'
                            file_url = event.get(url_field)
                            
                            if file_url:
                                success = False
                                
                                if doc_type == 'transcript':
                                    # Handle transcript
                                    transcripts = event.get('transcripts', {})
                                    if transcripts:
                                        transcript_text = await transcript_processor.process_transcript(
                                            file_url,
                                            transcripts,
                                            session
                                        )
                                        if transcript_text:
                                            pdf_bytes = transcript_processor.create_pdf(
                                                company_name,
                                                event_title,
                                                event_date,
                                                transcript_text
                                            )
                                            
                                            s3_key = format_s3_key(
                                                company_name,
                                                event_date,
                                                doc_type,
                                                f"{event_title.lower().replace(' ', '_')}_transcript.pdf"
                                            )
                                            
                                            success = await s3_handler.upload_file(
                                                pdf_bytes,
                                                s3_key,
                                                bucket_name
                                            )
                                elif doc_type == 'audio':
                                    # Handle audio file
                                    async with session.get(file_url) as response:
                                        if response.status == 200:
                                            content = await response.read()
                                            file_extension = file_url.split('.')[-1].lower()
                                            s3_key = format_s3_key(
                                                company_name,
                                                event_date,
                                                doc_type,
                                                f"{event_title.lower().replace(' ', '_')}.{file_extension}"
                                            )
                                            content_type = 'audio/mpeg' if file_extension == 'mpeg' else 'audio/mp3'
                                            success = await s3_handler.upload_file(
                                                content,
                                                s3_key,
                                                bucket_name,
                                                content_type
                                            )
                                else:
                                    # Handle regular files (slides, reports)
                                    async with session.get(file_url) as response:
                                        if response.status == 200:
                                            content = await response.read()
                                            s3_key = format_s3_key(
                                                company_name,
                                                event_date,
                                                doc_type,
                                                file_url.split('/')[-1]
                                            )
                                            success = await s3_handler.upload_file(
                                                content,
                                                s3_key,
                                                bucket_name,
                                                response.headers.get('content-type', 'application/pdf')
                                            )
                                
                                if success:
                                    # Generate the AWS URL
                                    aws_url = f"s3://{bucket_name}/{s3_key}"
                                    
                                    # Create Airtable record
                                    airtable_handler = AirtableHandler()
                                    airtable_success = await airtable_handler.create_record(
                                        company=company_name,
                                        isin=current_isin,
                                        aws_url=aws_url,
                                        event_date=event.get('eventDate', ''),
                                        event_type=event.get('eventType', {}).get('type', ''),
                                        document_type=doc_type
                                    )
                                    
                                    if airtable_success:
                                        successful_uploads += 1
                                    else:
                                        st.warning(f"Failed to create Airtable record for {s3_key}")
                                        failed_uploads += 1
                                else:
                                    failed_uploads += 1
                                
                                processed_files += 1
                                progress = processed_files / total_files
                                progress_bar.progress(progress)
                                status_text.text(f"Processing: {processed_files}/{total_files} files")
                                files_processed.text(
                                    f"Successful uploads: {successful_uploads} | "
                                    f"Failed uploads: {failed_uploads}"
                                )
                                
                                await asyncio.sleep(0.1)
            
            progress_bar.progress(1.0)
            status_text.text("Processing complete!")
            files_processed.text(
                f"Final results:\n"
                f"Total files processed: {processed_files}\n"
                f"Successful uploads: {successful_uploads}\n"
                f"Failed uploads: {failed_uploads}"
            )
            
            st.session_state.processing_complete = True
            
    except Exception as e:
        st.error(f"An error occurred during processing: {str(e)}")
        raise

def main():
    st.title("Quartr Data Retrieval and S3 Upload")
    
    # Example ISINs
    st.sidebar.header("Help")
    st.sidebar.markdown("""
    ### Example ISINs:
    - US5024413065 (LVMH ADR)
    - FR0000121014 (LVMH)
    - TH0809120700 (LVMH TH)
    
    Enter one ISIN per line in the input box.
    """)
    
    with st.form(key="quartr_form"):
        isin_input = st.text_area(
            "Enter ISINs (one per line)",
            help="Enter each ISIN on a new line. See sidebar for examples.",
            height=100
        )
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                datetime(2024, 1, 1),
                help="Select start date for document retrieval",
                min_value=datetime(2000, 1, 1)
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                datetime(2024, 12, 31),
                help="Select end date for document retrieval",
                max_value=datetime(2025, 12, 31)
            )
        
        doc_types = st.multiselect(
            "Select document types",
            ["slides", "report", "transcript", "audio"],
            default=["slides", "report", "transcript"],
            help="Choose which types of documents to retrieve"
        )
        
        # Get default bucket from secrets with fallback
        default_bucket = ""
        try:
            default_bucket = st.secrets["s3"]["DEFAULT_BUCKET"]
        except Exception:
            st.warning("No default bucket configured in secrets.toml")
            
        s3_bucket = st.text_input(
            "S3 Bucket Name",
            value=default_bucket,
            help="Enter the name of the S3 bucket for file upload"
        )
        
        submitted = st.form_submit_button("Start Processing")
        
        if submitted:
            if not isin_input or not s3_bucket or not doc_types:
                st.error("Please fill in all required fields")
                return
            
            if start_date > end_date:
                st.error("Start date must be before end date")
                return
            
            isin_list = [isin.strip() for isin in isin_input.split("\n") if isin.strip()]
            
            if not isin_list:
                st.error("Please enter at least one valid ISIN")
                return
            
            try:
                asyncio.run(process_documents(
                    isin_list,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    doc_types,
                    s3_bucket
                ))
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                return

if __name__ == "__main__":
    main()
    
