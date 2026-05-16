-- Extend sic_gics_mapping with the SIC codes that already appear in
-- the companies table but were not covered by 09_sic_gics_data.sql.
--
-- Sourced from the 2026-05-16 audit query:
--   SELECT DISTINCT c.sic_code, c.sic_description, COUNT(*)
--   FROM companies c LEFT JOIN sic_gics_mapping m USING(sic_code)
--   WHERE c.sic_code IS NOT NULL AND m.gics_sector IS NULL
--   GROUP BY c.sic_code, c.sic_description;
--
-- Covers 170 distinct SICs, ~1,629 companies. Each row uses the same
-- vocabulary (gics_sector and gics_industry names) as
-- 09_sic_gics_data.sql so the two seed files stay consistent.
--
-- Confidence:
--   high   = SIC description maps unambiguously to one GICS industry
--   medium = SIC description fits a GICS industry but the SIC is
--            broad enough to span multiple sub-industries
--   low    = SIC is a placeholder (e.g. 6770 BLANK CHECKS, mostly
--            SPACs that don't have a real industry) or genuinely
--            ambiguous

INSERT INTO sic_gics_mapping (sic_code, gics_sector, gics_industry, confidence, notes) VALUES
-- Agriculture, forestry, fishing
('0200', 'Consumer Staples', 'Agricultural Products', 'medium', 'Livestock production'),
('0700', 'Consumer Staples', 'Agricultural Products', 'medium', 'Agricultural services'),
('0900', 'Consumer Staples', 'Agricultural Products', 'low',    'Fishing, hunting, trapping'),

-- Mining
('1090', 'Materials', 'Metals & Mining', 'high', 'Miscellaneous metal ores'),
('1220', 'Energy', 'Oil & Gas E&P', 'high', 'Bituminous coal & lignite mining'),
('1221', 'Energy', 'Oil & Gas E&P', 'high', 'Bituminous coal surface mining'),

-- Oil & gas services
('1381', 'Energy', 'Oil & Gas Equipment', 'high', 'Drilling oil & gas wells'),
('1382', 'Energy', 'Oil & Gas Equipment', 'high', 'Oil & gas field exploration services'),

-- Construction
('1540', 'Industrials', 'Construction & Engineering', 'high', 'General building contractors - non-residential'),
('1623', 'Industrials', 'Construction & Engineering', 'high', 'Water/sewer/pipeline/comm/power line construction'),
('1700', 'Industrials', 'Construction & Engineering', 'high', 'Construction - special trade contractors'),

-- Food
('2013', 'Consumer Staples', 'Packaged Foods', 'high', 'Sausages & prepared meat'),
('2020', 'Consumer Staples', 'Packaged Foods', 'high', 'Dairy products'),
('2052', 'Consumer Staples', 'Packaged Foods', 'high', 'Cookies & crackers'),

-- Tobacco
('2100', 'Consumer Staples', 'Tobacco', 'high', 'Tobacco products'),

-- Textiles & apparel
('2200', 'Consumer Discretionary', 'Apparel', 'medium', 'Textile mill products'),
('2211', 'Consumer Discretionary', 'Apparel', 'medium', 'Broadwoven fabric mills, cotton'),
('2221', 'Consumer Discretionary', 'Apparel', 'medium', 'Broadwoven fabric mills, man-made fiber & silk'),
('2273', 'Consumer Discretionary', 'Household Durables', 'high', 'Carpets & rugs'),
('2330', 'Consumer Discretionary', 'Apparel', 'high', 'Women''s, misses'', and juniors outerwear'),

-- Lumber & wood
('2400', 'Materials', 'Forest Products', 'high', 'Lumber & wood products (no furniture)'),
('2430', 'Materials', 'Forest Products', 'high', 'Millwork, veneer, plywood'),
('2451', 'Consumer Discretionary', 'Homebuilding', 'high', 'Mobile homes'),

-- Furniture
('2510', 'Consumer Discretionary', 'Household Durables', 'high', 'Household furniture'),
('2511', 'Consumer Discretionary', 'Household Durables', 'high', 'Wood household furniture'),
('2520', 'Industrials', 'Building Products', 'medium', 'Office furniture'),
('2522', 'Industrials', 'Building Products', 'medium', 'Office furniture (no wood)'),
('2531', 'Industrials', 'Building Products', 'medium', 'Public building & related furniture'),

-- Paper & pulp
('2611', 'Materials', 'Paper Products', 'high', 'Pulp mills'),
('2631', 'Materials', 'Paper Products', 'high', 'Paperboard mills'),

-- Publishing & printing
('2721', 'Communication Services', 'Publishing', 'high', 'Periodicals publishing'),
('2731', 'Communication Services', 'Publishing', 'high', 'Books publishing'),
('2741', 'Communication Services', 'Publishing', 'high', 'Miscellaneous publishing'),
('2750', 'Industrials', 'Commercial Services', 'medium', 'Commercial printing'),
('2761', 'Industrials', 'Commercial Services', 'medium', 'Manifold business forms'),
('2780', 'Industrials', 'Commercial Services', 'medium', 'Blankbooks, looseleaf binders & bookbinding'),

-- Chemicals
('2820', 'Materials', 'Chemicals', 'high', 'Plastic material, synthetic resin/rubber, cellulose'),
('2833', 'Health Care', 'Pharmaceuticals', 'high', 'Medicinal chemicals & botanical products'),
('2890', 'Materials', 'Specialty Chemicals', 'high', 'Miscellaneous chemical products'),
('2891', 'Materials', 'Specialty Chemicals', 'high', 'Adhesives & sealants'),
('2990', 'Energy', 'Oil & Gas Refining', 'medium', 'Miscellaneous products of petroleum & coal'),

-- Rubber & plastics
('3011', 'Consumer Discretionary', 'Auto Parts', 'high', 'Tires & inner tubes'),
('3050', 'Materials', 'Chemicals', 'medium', 'Gaskets, packaging & sealing devices, rubber/plastics hose'),
('3060', 'Materials', 'Chemicals', 'medium', 'Fabricated rubber products NEC'),
('3080', 'Materials', 'Chemicals', 'medium', 'Miscellaneous plastics products'),
('3086', 'Materials', 'Chemicals', 'medium', 'Plastics foam products'),

-- Footwear
('3140', 'Consumer Discretionary', 'Footwear', 'high', 'Footwear (no rubber)'),

-- Glass, pottery, concrete
('3211', 'Materials', 'Construction Materials', 'high', 'Flat glass'),
('3221', 'Materials', 'Containers & Packaging', 'high', 'Glass containers'),
('3231', 'Materials', 'Construction Materials', 'high', 'Glass products from purchased glass'),
('3260', 'Consumer Discretionary', 'Household Durables', 'medium', 'Pottery & related products'),
('3272', 'Materials', 'Construction Materials', 'high', 'Concrete products, except block & brick'),
('3290', 'Materials', 'Construction Materials', 'medium', 'Abrasive, asbestos & misc nonmetallic mineral products'),

-- Primary metals
('3310', 'Materials', 'Steel', 'high', 'Steel works, blast furnaces & rolling mills'),
('3317', 'Materials', 'Steel', 'high', 'Steel pipe & tubes'),
('3330', 'Materials', 'Metals & Mining', 'high', 'Primary smelting & refining of nonferrous metals'),
('3341', 'Materials', 'Metals & Mining', 'high', 'Secondary smelting & refining of nonferrous metals'),
('3360', 'Materials', 'Metals & Mining', 'medium', 'Nonferrous foundries (castings)'),
('3390', 'Materials', 'Metals & Mining', 'medium', 'Miscellaneous primary metal products'),

-- Fabricated metal
('3412', 'Materials', 'Containers & Packaging', 'high', 'Metal shipping barrels, drums, kegs & pails'),
('3433', 'Industrials', 'Building Products', 'high', 'Heating equipment, except electric & warm air furnaces'),
('3440', 'Industrials', 'Building Products', 'medium', 'Fabricated structural metal products'),
('3442', 'Industrials', 'Building Products', 'high', 'Metal doors, sash, frames, moldings & trim'),
('3443', 'Industrials', 'Industrial Machinery', 'medium', 'Fabricated plate work (boiler shops)'),
('3460', 'Industrials', 'Industrial Machinery', 'medium', 'Metal forgings & stampings'),
('3470', 'Industrials', 'Industrial Machinery', 'medium', 'Coating, engraving & allied services'),

-- Industrial machinery
('3524', 'Consumer Discretionary', 'Leisure Products', 'high', 'Lawn & garden tractors & home lawn equipment'),
('3537', 'Industrials', 'Industrial Machinery', 'high', 'Industrial trucks, tractors, trailers & stackers'),
('3541', 'Industrials', 'Industrial Machinery', 'high', 'Machine tools, metal cutting types'),
('3555', 'Industrials', 'Industrial Machinery', 'high', 'Printing trades machinery & equipment'),
('3562', 'Industrials', 'Industrial Machinery', 'high', 'Ball & roller bearings'),
('3564', 'Industrials', 'Industrial Machinery', 'high', 'Industrial & commercial fans & blowers & air purifying'),
('3578', 'Information Technology', 'Technology Hardware', 'medium', 'Calculating & accounting machines (no electronic computers)'),
('3579', 'Information Technology', 'Technology Hardware', 'medium', 'Office machines NEC'),

-- Electrical equipment
('3634', 'Consumer Discretionary', 'Household Durables', 'high', 'Electric housewares & fans'),
('3640', 'Industrials', 'Electrical Equipment', 'high', 'Electric lighting & wiring equipment'),
('3651', 'Consumer Discretionary', 'Household Durables', 'high', 'Household audio & video equipment'),
('3661', 'Information Technology', 'Communications Equipment', 'high', 'Telephone & telegraph apparatus'),
('3677', 'Information Technology', 'Electronic Equipment', 'high', 'Electronic coils, transformers & inductors'),

-- Transportation equipment
('3713', 'Consumer Discretionary', 'Automobiles', 'high', 'Truck & bus bodies'),
('3715', 'Industrials', 'Construction Machinery', 'high', 'Truck trailers'),
('3716', 'Consumer Discretionary', 'Automobiles', 'high', 'Motor homes'),
('3751', 'Consumer Discretionary', 'Automobiles', 'high', 'Motorcycles, bicycles & parts'),
('3790', 'Industrials', 'Construction Machinery', 'medium', 'Miscellaneous transportation equipment'),

-- Instruments & misc manufacturing
('3821', 'Health Care', 'Life Sciences Tools', 'high', 'Laboratory apparatus & furniture'),
('3824', 'Industrials', 'Industrial Instruments', 'high', 'Totalizing fluid meters & counting devices'),
('3843', 'Health Care', 'Medical Equipment', 'high', 'Dental equipment & supplies'),
('3861', 'Industrials', 'Industrial Instruments', 'high', 'Photographic equipment & supplies'),
('3873', 'Consumer Discretionary', 'Apparel & Luxury', 'high', 'Watches, clocks, clockwork operated devices'),
('3910', 'Consumer Discretionary', 'Apparel & Luxury', 'high', 'Jewelry, silverware & plated ware'),
('3949', 'Consumer Discretionary', 'Leisure Products', 'high', 'Sporting & athletic goods NEC'),

-- Transportation
('4220', 'Industrials', 'Air Freight & Logistics', 'high', 'Public warehousing & storage'),
('4412', 'Industrials', 'Ground Transportation', 'high', 'Deep sea foreign transportation of freight'),
('4522', 'Industrials', 'Airlines', 'high', 'Air transportation, nonscheduled'),

-- Communications & utilities
('4822', 'Communication Services', 'Integrated Telecom', 'high', 'Telegraph & other message communications'),
('4900', 'Utilities', 'Multi-Utilities', 'high', 'Electric, gas & sanitary services'),
('4955', 'Industrials', 'Waste Management', 'high', 'Hazardous waste management'),

-- Wholesale trade (all → Industrials / Trading Companies)
('5010', 'Industrials', 'Trading Companies', 'high', 'Wholesale-motor vehicles & motor vehicle parts'),
('5030', 'Industrials', 'Trading Companies', 'high', 'Wholesale-lumber & other construction materials'),
('5031', 'Industrials', 'Trading Companies', 'high', 'Wholesale-lumber, plywood, millwork & wood panels'),
('5040', 'Industrials', 'Trading Companies', 'high', 'Wholesale-professional & commercial equipment'),
('5045', 'Industrials', 'Trading Companies', 'high', 'Wholesale-computers & peripheral equipment & software'),
('5050', 'Industrials', 'Trading Companies', 'high', 'Wholesale-metals & minerals (no petroleum)'),
('5051', 'Industrials', 'Trading Companies', 'high', 'Wholesale-metals service centers'),
('5063', 'Industrials', 'Trading Companies', 'high', 'Wholesale-electrical apparatus, wiring supplies'),
('5070', 'Industrials', 'Trading Companies', 'high', 'Wholesale-hardware & plumbing & heating'),
('5072', 'Industrials', 'Trading Companies', 'high', 'Wholesale-hardware'),
('5080', 'Industrials', 'Trading Companies', 'high', 'Wholesale-machinery, equipment & supplies'),
('5084', 'Industrials', 'Trading Companies', 'high', 'Wholesale-industrial machinery & equipment'),
('5094', 'Industrials', 'Trading Companies', 'high', 'Wholesale-jewelry, watches, precious stones'),
('5099', 'Industrials', 'Trading Companies', 'medium', 'Wholesale-durable goods NEC'),
('5130', 'Industrials', 'Trading Companies', 'high', 'Wholesale-apparel, piece goods & notions'),
('5141', 'Consumer Staples', 'Food Distributors', 'high', 'Wholesale-groceries, general line'),
('5150', 'Consumer Staples', 'Agricultural Products', 'high', 'Wholesale-farm product raw materials'),
('5160', 'Industrials', 'Trading Companies', 'high', 'Wholesale-chemicals & allied products'),
('5171', 'Energy', 'Oil & Gas Midstream', 'high', 'Wholesale-petroleum bulk stations & terminals'),
('5172', 'Energy', 'Oil & Gas Midstream', 'high', 'Wholesale-petroleum & petroleum products'),
('5180', 'Consumer Staples', 'Beverages', 'high', 'Wholesale-beer, wine & distilled alcoholic beverages'),
('5190', 'Industrials', 'Trading Companies', 'medium', 'Wholesale-miscellaneous nondurable goods'),

-- Retail
('5311', 'Consumer Discretionary', 'Broadline Retail', 'high', 'Retail-department stores'),
('5400', 'Consumer Staples', 'Food Retail', 'high', 'Retail-food stores'),
('5412', 'Consumer Staples', 'Food Retail', 'high', 'Retail-convenience stores'),
('5600', 'Consumer Discretionary', 'Apparel Retail', 'high', 'Retail-apparel & accessory stores'),
('5621', 'Consumer Discretionary', 'Apparel Retail', 'high', 'Retail-women''s clothing stores'),
('5661', 'Consumer Discretionary', 'Apparel Retail', 'high', 'Retail-shoe stores'),
('5712', 'Consumer Discretionary', 'Home Furnishing Retail', 'high', 'Retail-furniture stores'),
('5734', 'Consumer Discretionary', 'Computer & Electronics Retail', 'high', 'Retail-computer & computer software stores'),
('5940', 'Consumer Discretionary', 'Specialty Retail', 'medium', 'Retail-miscellaneous shopping goods stores'),
('5944', 'Consumer Discretionary', 'Specialty Retail', 'high', 'Retail-jewelry stores'),
('5945', 'Consumer Discretionary', 'Specialty Retail', 'high', 'Retail-hobby, toy & game shops'),
('5960', 'Consumer Discretionary', 'Internet Retail', 'high', 'Retail-nonstore retailers'),

-- Financials
('6029', 'Financials', 'Banks', 'high', 'Commercial banks NEC'),
('6036', 'Financials', 'Thrifts & Mortgage', 'high', 'Savings institutions, not federally chartered'),
('6099', 'Financials', 'Banks', 'high', 'Functions related to depository banking'),
('6111', 'Financials', 'Consumer Finance', 'high', 'Federal & federally-sponsored credit agencies'),
('6153', 'Financials', 'Consumer Finance', 'high', 'Short-term business credit institutions'),
('6159', 'Financials', 'Consumer Finance', 'high', 'Miscellaneous business credit institution'),
('6162', 'Financials', 'Mortgage Finance', 'high', 'Mortgage bankers & loan correspondents'),
('6221', 'Financials', 'Capital Markets', 'high', 'Commodity contracts brokers & dealers'),
('6351', 'Financials', 'Insurance', 'high', 'Surety insurance'),
('6361', 'Financials', 'Insurance', 'high', 'Title insurance'),

-- Real estate
('6512', 'Real Estate', 'Real Estate Services', 'high', 'Operators of nonresidential buildings'),
('6513', 'Real Estate', 'Real Estate Services', 'high', 'Operators of apartment buildings'),
('6519', 'Real Estate', 'Real Estate Services', 'high', 'Lessors of real property NEC'),
('6552', 'Real Estate', 'Real Estate Services', 'high', 'Land subdividers & developers (no cemeteries)'),

-- Holding/investment companies. 6770 is the SPAC catch-all — set
-- confidence=low so consumers can choose to exclude these from
-- sector aggregates.
('6770', 'Financials', 'Capital Markets', 'low', 'Blank checks — almost entirely SPACs (Special Purpose Acquisition Companies)'),
('6799', 'Financials', 'Capital Markets', 'medium', 'Investors NEC'),

-- Hotels & personal services
('7000', 'Consumer Discretionary', 'Hotels & Resorts', 'high', 'Hotels, rooming houses, camps & other lodging'),
('7200', 'Consumer Discretionary', 'Commercial Services', 'medium', 'Services-personal services'),

-- Advertising
('7310', 'Communication Services', 'Advertising', 'high', 'Services-advertising'),
('7330', 'Communication Services', 'Advertising', 'medium', 'Services-mailing, reproduction, commercial art & photography'),
('7331', 'Communication Services', 'Advertising', 'high', 'Services-direct mail advertising services'),

-- Business services
('7361', 'Industrials', 'Professional Services', 'high', 'Services-employment agencies'),
('7363', 'Industrials', 'Professional Services', 'high', 'Services-help supply services (staffing)'),
('7380', 'Industrials', 'Commercial Services', 'medium', 'Services-miscellaneous business services'),

-- Auto repair & misc repair
('7500', 'Consumer Discretionary', 'Commercial Services', 'high', 'Services-automotive repair, services & parking'),
('7600', 'Consumer Discretionary', 'Commercial Services', 'medium', 'Services-miscellaneous repair services'),

-- Movies & entertainment
('7812', 'Communication Services', 'Movies & Entertainment', 'high', 'Services-motion picture & video tape production'),
('7830', 'Communication Services', 'Movies & Entertainment', 'high', 'Services-motion picture theaters'),
('7997', 'Consumer Discretionary', 'Entertainment', 'high', 'Services-membership sports & recreation clubs'),

-- Health services
('8000', 'Health Care', 'Health Care Services', 'high', 'Services-health services'),
('8050', 'Health Care', 'Health Care Facilities', 'high', 'Services-nursing & personal care facilities'),
('8060', 'Health Care', 'Health Care Facilities', 'high', 'Services-hospitals'),
('8093', 'Health Care', 'Health Care Facilities', 'high', 'Services-specialty outpatient facilities NEC'),

-- Professional services
('8111', 'Industrials', 'Professional Services', 'high', 'Services-legal services'),
('8351', 'Consumer Discretionary', 'Commercial Services', 'high', 'Services-child day care services'),
('8734', 'Industrials', 'Research & Consulting', 'high', 'Services-testing laboratories'),
('8742', 'Industrials', 'Professional Services', 'high', 'Services-management consulting services'),
('8744', 'Industrials', 'Commercial Services', 'high', 'Services-facilities support management services'),
('8900', 'Industrials', 'Commercial Services', 'low', 'Services-services NEC')
ON CONFLICT (sic_code) DO NOTHING;
