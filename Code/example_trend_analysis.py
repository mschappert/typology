from TimeSeries_MannKendall import local_trend_analysis, plot_trend_result
import glob

# Example: Analyze all TIFF files in a directory
data_dir = "path/to/your/raster/files"
file_paths = sorted(glob.glob(f"{data_dir}/*.tif"))

# Run trend analysis
stack, tau_result = local_trend_analysis(
    file_paths=file_paths,
    output_path="mann_kendall_trend_output.tif"
)

# Create visualization
plot_trend_result(tau_result.values, "trend_analysis_plot.png")

print("Trend analysis complete!")
print(f"Output raster: mann_kendall_trend_output.tif")
print(f"Plot: trend_analysis_plot.png")