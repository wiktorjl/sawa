#!/usr/bin/env python3
"""
Sort CAT_150.md data by distance from 150-day SMA
"""
import re
from pathlib import Path

def parse_markdown_table(md_file):
    """Parse the markdown file and extract data"""
    with open(md_file, 'r') as f:
        content = f.read()

    # Find the full historical data table
    pattern = r'\| (\d{4}-\d{2}-\d{2}) \| \$([0-9.]+) \| \$([0-9.]+) \| ([+\-]\$[0-9.]+) \| ([+\-][0-9.]+)%'
    matches = re.findall(pattern, content)

    data = []
    for match in matches:
        date_str, price, sma, dist_dollars, dist_pct = match
        data.append({
            'date': date_str,
            'price': float(price),
            'sma_150': float(sma),
            'dist_dollars': dist_dollars,
            'dist_pct': float(dist_pct)
        })

    return data

# Parse data
md_file = Path('/home/user/code/sawa/CAT_150.md')
data = parse_markdown_table(md_file)

# Sort by distance percentage (ascending - most negative first)
sorted_data = sorted(data, key=lambda x: x['dist_pct'])

# Create sorted markdown table
print("\n# CAT Stock - Sorted by Distance from 150-Day SMA\n")
print("## Most Below SMA (Bottom 20)\n")
print("| Rank | Date | Price | 150-Day SMA | Distance ($) | Distance (%) |")
print("|------|------|-------|-------------|--------------|--------------|")

for i, row in enumerate(sorted_data[:20], 1):
    print(f"| {i} | {row['date']} | ${row['price']:.2f} | ${row['sma_150']:.2f} | {row['dist_dollars']} | {row['dist_pct']:+.2f}% |")

print("\n## Most Above SMA (Top 20)\n")
print("| Rank | Date | Price | 150-Day SMA | Distance ($) | Distance (%) |")
print("|------|------|-------|-------------|--------------|--------------|")

for i, row in enumerate(sorted_data[-20:][::-1], 1):
    print(f"| {i} | {row['date']} | ${row['price']:.2f} | ${row['sma_150']:.2f} | {row['dist_dollars']} | {row['dist_pct']:+.2f}% |")

# Create complete sorted table
print("\n## Complete Data Sorted by Distance (Ascending)\n")
print("| Rank | Date | Price | 150-Day SMA | Distance ($) | Distance (%) |")
print("|------|------|-------|-------------|--------------|--------------|")

for i, row in enumerate(sorted_data, 1):
    print(f"| {i} | {row['date']} | ${row['price']:.2f} | ${row['sma_150']:.2f} | {row['dist_dollars']} | {row['dist_pct']:+.2f}% |")
