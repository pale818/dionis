import pandas as pd
import matplotlib
# Force Matplotlib to not use any X-windows backend (fixes Tkinter errors)
matplotlib.use('Agg') 
import matplotlib.pyplot as plt
import os

def create_viz():
    csv_path = "data/csv/bird_statistics_report.csv"
    output_dir = "data/visualizations"
    output_file = f"{output_dir}/sightings_chart.png"

    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Read the data
    df = pd.read_csv(csv_path)
    
    # Sort by sightings and take Top 10
    top_10 = df.sort_values(by="Total Sightings", ascending=False).head(10)

    # Create the plot
    plt.figure(figsize=(12, 7))
    plt.bar(top_10["Common Name"], top_10["Total Sightings"], color='forestgreen')
    
    plt.xlabel("Bird Species", fontsize=12)
    plt.ylabel("Number of Sightings", fontsize=12)
    plt.title("Top 10 Most Observed Bird Species - DIONIS Project", fontsize=14)
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    # Save the file - Snakemake will look for this output
    plt.savefig(output_file)
    plt.close() # Close to free up memory
    print(f"Visualization created: {output_file}")

if __name__ == "__main__":
    create_viz()