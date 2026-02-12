-- Add NASDAQ-1000 index to the indices table
INSERT INTO indices (code, name, description, source_url) VALUES
('nasdaq1000', 'NASDAQ-1000', 'NASDAQ-1000 Index - 1000 largest NASDAQ stocks',
 'Custom list')
ON CONFLICT (code) DO NOTHING;
