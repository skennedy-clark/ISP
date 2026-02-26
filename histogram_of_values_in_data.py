import sqlite3
import matplotlib.pyplot as plt
import numpy as np

# Connect to the database
conn = sqlite3.connect(r'db\Supply-annual.db')

# Query the data
query = "SELECT Value FROM data WHERE Value != 'x'"
cursor = conn.cursor()
cursor.execute(query)
data = cursor.fetchall()
conn.close()

# Convert to numeric array
values = [float(row[0]) for row in data]

# Print statistics
print(f"Data points: {len(values):,}")
print(f"Maximum: {max(values):,.2f}")
print(f"Minimum: {min(values):,.2f}")
print(f"Average: {np.mean(values):,.2f}")
print(f"Median: {np.median(values):,.2f}")
print(f"Std Dev: {np.std(values):,.2f}")

# Create the histogram
fig, ax = plt.subplots(figsize=(14, 8))

# Create histogram with larger bins (20 bins instead of 60)
# Using the range from -210,000 to 300,000
counts, bins, patches = ax.hist(values, bins=20, edgecolor='black', alpha=0.75, color='steelblue')

# Set logarithmic y-scale
ax.set_yscale('log')

# Color bars based on positive/negative values
for i, patch in enumerate(patches):
    if bins[i] < 0:
        patch.set_facecolor('#E63946')  # Red for negative
    else:
        patch.set_facecolor('#06A77D')  # Green for positive

# Add reference lines
ax.axvline(np.mean(values), color='darkblue', linestyle='--', linewidth=2, 
           label=f'Mean: {np.mean(values):,.2f}', alpha=0.8)
ax.axvline(np.median(values), color='purple', linestyle=':', linewidth=2, 
           label=f'Median: {np.median(values):,.2f}', alpha=0.8)
ax.axvline(0, color='black', linestyle='-', linewidth=1, alpha=0.3)

# Add statistics text box
stats_text = (f"Statistics\n"
              f"{'='*25}\n"
              f"Count:    {len(values):>15,}\n"
              f"Maximum:  {max(values):>15,.2f}\n"
              f"Minimum:  {min(values):>15,.2f}\n"
              f"Mean:     {np.mean(values):>15,.2f}\n"
              f"Median:   {np.median(values):>15,.2f}\n"
              f"Std Dev:  {np.std(values):>15,.2f}\n"
              f"Range:    {max(values) - min(values):>15,.2f}")

ax.text(0.98, 0.97, stats_text, transform=ax.transAxes, 
        verticalalignment='top', horizontalalignment='right',
        bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.9, edgecolor='black'),
        fontsize=10, family='monospace', fontweight='bold')

# Formatting
ax.set_xlabel('Value', fontsize=13, fontweight='bold')
ax.set_ylabel('Frequency (log scale)', fontsize=13, fontweight='bold')
ax.set_title('Seabourn Data Distribution Histogram (Log Scale)\nSupply Annual Database', 
             fontsize=16, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3, linestyle='--', linewidth=0.7)
ax.legend(loc='upper left', fontsize=11, framealpha=0.9)

# Format axes with commas
ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:,.0f}'))
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))

# Set x-axis range based on your calibration values
ax.set_xlim(-220000, 310000)

ax.set_facecolor('#F8F9FA')

plt.tight_layout()
plt.savefig('seabourn_histogram.png', dpi=300, bbox_inches='tight', facecolor='white')
print("\n✓ Histogram saved as 'seabourn_histogram.png'")
plt.show()