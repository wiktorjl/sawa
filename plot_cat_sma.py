#!/usr/bin/env python3
"""
Plot CAT stock price vs 150-day SMA
"""
import re
from datetime import datetime
import plotext as plt
from pathlib import Path

def parse_markdown_table(md_file):
    """Parse the markdown file and extract data"""
    with open(md_file, 'r') as f:
        content = f.read()

    # Find the full historical data table
    pattern = r'\| (\d{4}-\d{2}-\d{2}) \| \$([0-9.]+) \| \$([0-9.]+) \| [+\-]\$[0-9.]+ \| ([+\-][0-9.]+)%'
    matches = re.findall(pattern, content)

    dates = []
    prices = []
    sma_150 = []
    distance_pct = []

    for match in matches:
        date_str, price, sma, dist_pct = match
        dates.append(datetime.strptime(date_str, '%Y-%m-%d'))
        prices.append(float(price))
        sma_150.append(float(sma))
        distance_pct.append(float(dist_pct))

    return dates, prices, sma_150, distance_pct

# Parse data
md_file = Path('/home/user/code/sawa/CAT_150.md')
dates, prices, sma_150, distance_pct = parse_markdown_table(md_file)

# Reverse to have chronological order
dates = dates[::-1]
prices = prices[::-1]
sma_150 = sma_150[::-1]
distance_pct = distance_pct[::-1]

# Create x-axis as numeric indices
x_indices = list(range(len(dates)))

# Create date labels for selected points
step = max(len(dates) // 12, 1)
x_ticks = [i for i in range(0, len(dates), step)]
x_labels = [dates[i].strftime('%Y-%m-%d') for i in x_ticks]

print("\n" + "="*100)
print("CATERPILLAR INC. (CAT) - PRICE VS 150-DAY SMA")
print("="*100 + "\n")

# Plot 1: Price and SMA
plt.clf()
plt.plot(x_indices, prices, label='CAT Price', color='cyan+')
plt.plot(x_indices, sma_150, label='150-Day SMA', color='magenta+')
plt.title('CAT Price vs 150-Day SMA')
plt.xlabel('Date')
plt.ylabel('Price ($)')
plt.theme('dark')
plt.xticks(x_ticks, x_labels)
plt.ylim(250, max(prices) * 1.05)
plt.plotsize(140, 30)
plt.show()

print("\n" + "="*100)
print("DISTANCE FROM 150-DAY SMA (%)")
print("="*100 + "\n")

# Plot 2: Distance percentage
plt.clf()
# Color bars based on positive/negative
bar_colors = ['green+' if d >= 0 else 'red+' for d in distance_pct]
for i in range(len(x_indices)):
    plt.bar([x_indices[i]], [distance_pct[i]], color=bar_colors[i], width=0.8)

plt.title('Distance from 150-Day SMA (%)')
plt.xlabel('Date')
plt.ylabel('Distance (%)')
plt.theme('dark')
plt.xticks(x_ticks, x_labels)
plt.plotsize(140, 25)
plt.show()

# Print statistics
print("\n" + "="*100)
print("STATISTICS")
print("="*100)
print(f"\nLatest Data (2026-02-10):")
print(f"  Price:        ${prices[-1]:.2f}")
print(f"  SMA-150:      ${sma_150[-1]:.2f}")
print(f"  Distance:     {distance_pct[-1]:+.2f}%")
print(f"\nExtreme Values:")
print(f"  Max Above:    {max(distance_pct):+.2f}%")
print(f"  Max Below:    {min(distance_pct):+.2f}%")
print(f"  Date Range:   {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")
print(f"  Data Points:  {len(dates)}")
print("="*100 + "\n")
