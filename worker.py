import time
import os
import openai
import pandas as pd
from supabase import create_client, Client
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import tempfile
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # Service key for server-side
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
SMTP_EMAIL = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# Initialize clients
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
perplexity_client = openai.OpenAI(
    api_key=PERPLEXITY_API_KEY,
    base_url="https://api.perplexity.ai"
)

class JobProcessor:
    def __init__(self, job_id):
        self.job_id = job_id
        self.job_data = None
        
    def log_progress(self, message):
        """Log progress to database and console"""
        logger.info(f"Job {self.job_id}: {message}")
        supabase.table("progress_logs").insert({
            "job_id": self.job_id,
            "message": message
        }).execute()
    
    def update_progress(self, processed, total, percentage):
        """Update job progress in database"""
        supabase.table("jobs").update({
            "processed_companies": processed,
            "total_companies": total,
            "progress_percentage": percentage
        }).eq("id", self.job_id).execute()
    
    def update_status(self, status, error_message=None):
        """Update job status"""
        update_data = {"status": status}
        if error_message:
            update_data["error_message"] = error_message
        if status == "processing":
            update_data["started_at"] = "NOW()"
        elif status in ["completed", "failed"]:
            update_data["completed_at"] = "NOW()"
            
        supabase.table("jobs").update(update_data).eq("id", self.job_id).execute()
    
    def load_job_data(self):
        """Load job details from database"""
        response = supabase.table("jobs").select("*").eq("id", self.job_id).execute()
        if not response.data:
            raise Exception(f"Job {self.job_id} not found")
        self.job_data = response.data[0]
        return self.job_data
    
    def download_input_file(self):
        """Download the input CSV from Supabase storage"""
        file_path = self.job_data["input_file_path"]
        response = supabase.storage.from_("uploads").download(file_path)
        return response
    
    def analyze_companies_batch(self, batch_accounts):
        """Your existing Perplexity analysis logic"""
        company_list = []
        for i, account in enumerate(batch_accounts, 1):
            company_name = account.get(self.job_data["company_name_column"], "Unknown")
            website = account.get(self.job_data["website_column"], "No website")
            company_list.append(f"{i}. {company_name} ({website})")
        
        companies_text = "\n".join(company_list)
        
        prompt = f"""
Analyze these {len(batch_accounts)} companies and summarize only if any of these are true for each:
    • Acquired, merged, renamed, shut down
    • Has subsidiaries  
    • Is a government, academic, or healthcare organization

Companies to analyze:
{companies_text}

For each company, output ONLY a one-line status using these templates:
    • Company is a subsidiary of Parent
    • Acquired by Buyer in Year
    • Renamed to New Name
    • Shut down
    • Academic/government/healthcare: not sellable
    • No acquisitions, merges, renames, shutdowns, or subsidiaries

Format your response as:
1. [Status for company 1]
2. [Status for company 2]
3. [Status for company 3]
...

Write nothing else. No explanations, just numbered statuses.
"""
        
        for attempt in range(3):  # Max retries
            try:
                response = perplexity_client.chat.completions.create(
                    model="sonar",
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant that analyzes company statuses."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.3,
                    max_tokens=2000
                )
                return response.choices[0].message.content.strip()
            except Exception as e:
                self.log_progress(f"API attempt {attempt + 1} failed: {e}")
                if attempt == 2:
                    return None
                time.sleep(2 ** attempt)
    
    def parse_batch_results(self, batch_response, batch_accounts):
        """Parse batch response and match to accounts"""
        if not batch_response:
            return ["PERPLEXITY ERROR"] * len(batch_accounts)
        
        results = []
        lines = batch_response.split('\n')
        
        for i in range(len(batch_accounts)):
            found_result = False
            for line in lines:
                line = line.strip()
                if line.startswith(f"{i+1}."):
                    result = line[len(f"{i+1}."):].strip()
                    results.append(result)
                    found_result = True
                    break
            
            if not found_result:
                results.append("PARSING ERROR - MANUAL REVIEW NEEDED")
        
        return results
    
    def process_companies(self):
        """Main processing logic"""
        try:
            self.log_progress("Starting company analysis...")
            self.update_status("processing")
            
            # Download and load CSV
            csv_data = self.download_input_file()
            df = pd.read_csv(tempfile.NamedTemporaryFile(delete=False), data=csv_data)
            
            total_companies = len(df)
            self.update_progress(0, total_companies, 0)
            
            # Process in batches
            batch_size = 3
            results_list = []
            
            for i in range(0, total_companies, batch_size):
                batch = df.iloc[i:i+batch_size].to_dict('records')
                batch_num = i // batch_size + 1
                total_batches = (total_companies + batch_size - 1) // batch_size
                
                self.log_progress(f"Processing batch {batch_num}/{total_batches}")
                
                # Analyze batch
                batch_response = self.analyze_companies_batch(batch)
                results = self.parse_batch_results(batch_response, batch)
                results_list.extend(results)
                
                # Update progress
                processed = min(i + batch_size, total_companies)
                percentage = (processed / total_companies) * 100
                self.update_progress(processed, total_companies, percentage)
                
                # Rate limiting
                if batch_num < total_batches:
                    time.sleep(1.4)
            
            # Add results to dataframe
            df["scrub_summary"] = results_list
            
            # Save results CSV
            output_filename = f"results_{self.job_id}.csv"
            output_path = f"/tmp/{output_filename}"
            df.to_csv(output_path, index=False)
            
            # Upload to Supabase storage
            with open(output_path, 'rb') as f:
                supabase.storage.from_("results").upload(output_filename, f)
            
            # Update job with output file path
            supabase.table("jobs").update({
                "output_file_path": output_filename
            }).eq("id", self.job_id).execute()
            
            self.log_progress("Analysis completed successfully!")
            self.update_status("completed")
            
            # Send email
            self.send_completion_email(output_path)
            
        except Exception as e:
            self.log_progress(f"Error processing job: {str(e)}")
            self.update_status("failed", str(e))
            raise
    
    def send_completion_email(self, csv_path):
        """Send completion email with CSV attachment"""
        try:
            msg = MIMEMultipart()
            msg['From'] = SMTP_EMAIL
            msg['To'] = self.job_data['email']
            msg['Subject'] = "Your Company Analysis is Complete!"
            
            body = f"""
            Your company analysis job has completed successfully!
            
            Total companies processed: {self.job_data['total_companies']}
            
            Please find your results attached.
            
            You can also download your results from the dashboard.
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach CSV
            with open(csv_path, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= results_{self.job_id}.csv'
                )
                msg.attach(part)
            
            # Send email
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.send_message(msg)
            server.quit()
            
            self.log_progress("Completion email sent successfully")
            
        except Exception as e:
            self.log_progress(f"Failed to send email: {str(e)}")

def poll_for_jobs():
    """Main worker loop - polls for pending jobs"""
    logger.info("Starting job worker...")
    
    while True:
        try:
            # Look for pending jobs
            response = supabase.table("jobs").select("id").eq("status", "pending").limit(1).execute()
            
            if response.data:
                job_id = response.data[0]["id"]
                logger.info(f"Found pending job: {job_id}")
                
                processor = JobProcessor(job_id)
                processor.load_job_data()
                processor.process_companies()
                
            else:
                # No jobs found, wait before checking again
                time.sleep(10)
                
        except Exception as e:
            logger.error(f"Error in job worker: {str(e)}")
            time.sleep(30)  # Wait longer on error

if __name__ == "__main__":
    poll_for_jobs()