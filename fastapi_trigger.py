from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import subprocess

app = FastAPI()
scheduler = BackgroundScheduler()
scheduler.start()

def dbt_task():
    try:
        result = subprocess.run(["dbt", "run"], 
                              capture_output=True, 
                              text=True,
                              check=True)
        
        print(f"dbt started at: {datetime.now()}")
        print(f"Output: {result.stdout}")
        
        return {
            "status": "success",
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "output": result.stdout
        }
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return {
            "status": "error",
            "error": e.stderr
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

@app.get("/dbtstart")
async def start_dbt():
    scheduler.remove_all_jobs()
    
    scheduler.add_job(
        dbt_task,
        'interval',
        hours=12,
        id='scraping_job',
        next_run_time=datetime.now()
    )
    
    return {
        "message": "dbt task scheduled",
        "frequency": "Every 12 hours",
        "first_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }