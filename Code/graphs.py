# using docker image: basegis
# 

import pandas as pd
import matplotlib.pyplot as plt

# Load your data from Excel
# Replace 'your_file.xlsx' with the actual filename
# Replace 'Time_Segment' with the actual column name that contains "TS", "90-95", etc.
df = pd.read_excel('your_file.xlsx', index_col=0)

colors = {
    "Aggregation": "#9E0142",
    "Attrition": "#61945A",
    "Creation": "#F46D43",
    "Dissection": "#5E4FA2",
    "Fragmentation per se": "#00A9E6",
    "shirnkage": "#66C2A5",
    "perforation": "#3288BD",
    "enlargement": "#D53E4F",
    "shift": "#E6F598",
    "deformation": "#FEE08B"
}

time_points = df.columns.tolist()
typologies = df.index.tolist()

fig, ax = plt.subplots(figsize=(14, 8))

bottom = [0] * len(time_points)
for typ in typologies:
    data = df.loc[typ]
    ax.bar(
        time_points,
        data,
        bottom=bottom,
        color=colors.get(typ, "#cccccc"),
        edgecolor='none',
        label=typ
    )
    # Update bottom for stacking
    bottom = [bottom[i] + data[i] for i in range(len(data))]

ax.set_xlabel('Time Segments')
ax.set_ylabel('Values')
ax.set_xticks(range(len(time_points)))
ax.set_xticklabels(time_points)
ax.margins(x=0)
ax.legend(title='Typology Type', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
