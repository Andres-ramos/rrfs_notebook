import subprocess 
import pandas as pd


start_day = pd.Timestamp(2023,5,9)
end_date = pd.Timestamp(2023,6,30)

for date in pd.date_range(start=start_day, end=end_date, freq="D"):
    day = date.strftime("%Y-%m-%d")
    result = subprocess.run(["sbatch", "day_script.sh", day, 3, "3t1"], capture_output=True, text=True)
    result = subprocess.run(["sbatch", "day_script.sh", day, 3, "9t1"], capture_output=True, text=True)
    result = subprocess.run(["sbatch", "day_script.sh", day, 15, "3t1"], capture_output=True, text=True)
    result = subprocess.run(["sbatch", "day_script.sh", day, 15, "9t1"], capture_output=True, text=True)





