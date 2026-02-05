-- ============================================
-- SIC TO GICS MAPPING DATA
-- ============================================
-- Initial data load for sic_gics_mapping table
-- Based on Yahoo Finance sector classifications and SEC SIC code descriptions

INSERT INTO sic_gics_mapping (sic_code, gics_sector, gics_industry, confidence, notes) VALUES
-- =========================================================================
-- INFORMATION TECHNOLOGY
-- =========================================================================
-- Software (737x)
('7372', 'Information Technology', 'Software', 'high', 'Validated: MSFT, ORCL, CRM, ADBE all map to IT/Software'),
('7371', 'Information Technology', 'IT Services', 'high', 'Computer programming services - CTSH, EPAM'),
('7373', 'Information Technology', 'IT Services', 'high', 'Computer integrated systems design - SAIC, LDOS'),
('7374', 'Information Technology', 'IT Services', 'high', 'Data processing services - ADP, WDAY'),
-- Semiconductors (367x)
('3674', 'Information Technology', 'Semiconductors', 'high', 'Validated: NVDA, AMD, INTC, AVGO all map to IT/Semiconductors'),
('3672', 'Information Technology', 'Electronic Equipment', 'high', 'Printed circuit boards - FLEX, JBL'),
('3670', 'Information Technology', 'Electronic Equipment', 'medium', 'Electronic components & accessories - HUBB, OLED'),
('3679', 'Information Technology', 'Electronic Equipment', 'high', 'Electronic components NEC - power electronics'),
('3678', 'Information Technology', 'Electronic Equipment', 'high', 'Electronic connectors - APH'),
('3663', 'Information Technology', 'Communications Equipment', 'high', 'Radio/TV broadcasting equipment - QCOM, MSI'),
('3669', 'Information Technology', 'Communications Equipment', 'high', 'Communications equipment NEC - LITE'),
-- Computer Hardware (357x)
('3571', 'Information Technology', 'Technology Hardware', 'high', 'Electronic computers - AAPL, DELL, SMCI'),
('3572', 'Information Technology', 'Technology Hardware', 'high', 'Computer storage devices - WDC, STX, NTAP'),
('3570', 'Information Technology', 'Technology Hardware', 'high', 'Computer & office equipment - HPE, HPQ, IBM'),
('3576', 'Information Technology', 'Communications Equipment', 'high', 'Computer communications equipment - CSCO, ANET'),
('3577', 'Information Technology', 'Technology Hardware', 'medium', 'Computer peripherals - includes PANW, FTNT (cybersecurity software)'),
-- Semiconductor Equipment
('3559', 'Information Technology', 'Semiconductor Equipment', 'high', 'Special industry machinery NEC - LRCX is semiconductor equipment'),
('3827', 'Information Technology', 'Semiconductor Equipment', 'high', 'Optical instruments & lenses - KLAC semiconductor inspection'),
-- IT Services & Consulting
('8741', 'Information Technology', 'IT Services', 'high', 'Management services - IT (Gartner) is IT research/consulting'),
('6794', 'Information Technology', 'Technology Hardware', 'medium', 'Patent owners & lessors - IDCC is technology licensing'),
-- Other IT
('3357', 'Information Technology', 'Electronic Equipment', 'high', 'Drawing & insulating of nonferrous wire - GLW (Corning) fiber optics'),

-- =========================================================================
-- COMMUNICATION SERVICES
-- =========================================================================
-- Internet/Media (7370 - special case)
('7370', 'Communication Services', 'Interactive Media', 'high', 'Validated: GOOG, META are Communication Services per Yahoo Finance'),
-- Telecommunications (481x)
('4813', 'Communication Services', 'Integrated Telecom', 'high', 'Telephone communications - T, VZ'),
('4812', 'Communication Services', 'Wireless Telecom', 'high', 'Radiotelephone communications - TMUS'),
('4899', 'Communication Services', 'Wireless Telecom', 'medium', 'Communications services NEC - satellite companies ASTS, GSAT'),
-- Broadcasting & Cable (483x, 484x)
('4833', 'Communication Services', 'Broadcasting', 'high', 'Television broadcasting - FOX, NXST'),
('4832', 'Communication Services', 'Broadcasting', 'high', 'Radio broadcasting - SIRI'),
('4841', 'Communication Services', 'Cable & Satellite', 'high', 'Cable TV services - CMCSA, CHTR'),
-- Publishing (271x)
('2711', 'Communication Services', 'Publishing', 'high', 'Newspapers - NWS, NWSA'),
-- Entertainment
('7841', 'Communication Services', 'Movies & Entertainment', 'high', 'Video tape rental (streaming) - NFLX'),
('7900', 'Communication Services', 'Entertainment', 'high', 'Amusement & recreation services - LYV, TKO, WMG are media/entertainment'),
('7311', 'Communication Services', 'Advertising', 'high', 'Advertising agencies - OMC'),

-- =========================================================================
-- HEALTH CARE
-- =========================================================================
-- Pharmaceuticals (283x)
('2834', 'Health Care', 'Pharmaceuticals', 'high', 'Pharmaceutical preparations - JNJ, PFE, LLY, MRK'),
('2836', 'Health Care', 'Biotechnology', 'high', 'Biological products - AMGN, GILD, BIIB, MRNA'),
('2835', 'Health Care', 'Life Sciences Tools', 'high', 'Diagnostic substances - IDXX'),
-- Medical Devices (384x)
('3841', 'Health Care', 'Medical Devices', 'high', 'Surgical & medical instruments - BDX, SYK, BSX'),
('3842', 'Health Care', 'Medical Devices', 'high', 'Orthopedic & surgical appliances - ISRG, ZBH, EW'),
('3844', 'Health Care', 'Medical Equipment', 'high', 'X-ray apparatus - GEHC, HOLX'),
('3845', 'Health Care', 'Medical Devices', 'high', 'Electromedical apparatus - MDT, MASI'),
('3851', 'Health Care', 'Medical Devices', 'high', 'Ophthalmic goods - COO'),
-- Life Sciences Tools & Services
('3826', 'Health Care', 'Life Sciences Tools', 'high', 'Laboratory analytical instruments - A, ILMN, WAT, TMO'),
('8071', 'Health Care', 'Life Sciences Tools', 'high', 'Medical laboratories - LH, DGX'),
('8731', 'Health Care', 'Life Sciences Tools', 'high', 'Commercial physical & biological research - IQV, CRL'),
-- Healthcare Services & Facilities
('6324', 'Health Care', 'Managed Health Care', 'high', 'Hospital & medical service plans - UNH, CI, ELV, HUM'),
('8062', 'Health Care', 'Health Care Facilities', 'high', 'General medical & surgical hospitals - HCA, UHS'),
('8082', 'Health Care', 'Health Care Services', 'high', 'Home health care services - OPCH'),
('8090', 'Health Care', 'Health Care Services', 'high', 'Misc health & allied services - DVA dialysis'),
('8011', 'Health Care', 'Health Care Services', 'high', 'Offices & clinics of doctors'),
('8051', 'Health Care', 'Health Care Facilities', 'high', 'Skilled nursing care facilities - ENSG'),
-- Healthcare Distributors
('5122', 'Health Care', 'Health Care Distributors', 'high', 'Wholesale drugs - MCK, CAH, COR'),
('5047', 'Health Care', 'Health Care Distributors', 'high', 'Wholesale medical equipment - HSIC'),

-- =========================================================================
-- FINANCIALS
-- =========================================================================
-- Banks (602x)
('6022', 'Financials', 'Banks', 'high', 'State commercial banks - regional banks'),
('6021', 'Financials', 'Banks', 'high', 'National commercial banks - JPM, BAC, WFC, C'),
('6035', 'Financials', 'Thrifts & Mortgage', 'high', 'Savings institutions - EBC'),
-- Insurance (631x, 632x, 633x, 64xx)
('6331', 'Financials', 'Property & Casualty Insurance', 'high', 'Fire, marine & casualty insurance - BRK, AIG, ALL, TRV'),
('6311', 'Financials', 'Life Insurance', 'high', 'Life insurance - MET, PRU'),
('6321', 'Financials', 'Life & Health Insurance', 'high', 'Accident & health insurance - AFL'),
('6411', 'Financials', 'Insurance Brokers', 'high', 'Insurance agents & brokers - AON, AJG, MMC, WTW'),
('6399', 'Financials', 'Insurance', 'high', 'Insurance carriers NEC - AIZ'),
-- Capital Markets (620x, 621x, 628x)
('6211', 'Financials', 'Capital Markets', 'high', 'Security brokers & dealers - GS, MS, SCHW'),
('6200', 'Financials', 'Capital Markets', 'high', 'Security & commodity exchanges - CME, ICE, NDAQ'),
('6282', 'Financials', 'Asset Management', 'high', 'Investment advice - BLK, BX, KKR, APO'),
-- Consumer Finance
('6141', 'Financials', 'Consumer Finance', 'high', 'Personal credit institutions - AFRM, SLM'),
('6163', 'Financials', 'Mortgage Finance', 'high', 'Loan brokers'),
-- Financial Services
('6199', 'Financials', 'Financial Services', 'medium', 'Finance services - mixed: AXP, SOFI, also crypto miners'),
-- Business Services with Financial focus (7389 - ambiguous)
('7389', 'Financials', 'Transaction Processing', 'medium', 'AMBIGUOUS: V, MA, PYPL (Financials), ACN (IT), UBER (Consumer Disc)'),

-- =========================================================================
-- REAL ESTATE
-- =========================================================================
('6798', 'Real Estate', 'REITs', 'high', 'Real estate investment trusts - PLD, AMT, EQIX, SPG'),
('6531', 'Real Estate', 'Real Estate Services', 'high', 'Real estate agents & managers - CBRE'),
('6500', 'Real Estate', 'Real Estate Services', 'high', 'Real estate - CBRE'),
('6510', 'Real Estate', 'Real Estate Services', 'high', 'Real estate operators & lessors - INVH'),

-- =========================================================================
-- UTILITIES
-- =========================================================================
-- Electric Utilities (491x)
('4911', 'Utilities', 'Electric Utilities', 'high', 'Electric services - NEE, SO, DUK, D'),
('4931', 'Utilities', 'Multi-Utilities', 'high', 'Electric & other services combined - DUK, EXC'),
('4991', 'Utilities', 'Independent Power', 'high', 'Cogeneration services - AES'),
-- Gas Utilities (492x)
('4922', 'Utilities', 'Gas Utilities', 'medium', 'Natural gas transmission - KMI, WMB (often midstream Energy)'),
('4923', 'Utilities', 'Gas Utilities', 'high', 'Natural gas transmission & distribution - OKE'),
('4924', 'Utilities', 'Gas Utilities', 'high', 'Natural gas distribution - ATO'),
('4932', 'Utilities', 'Multi-Utilities', 'high', 'Gas & other services combined - SRE'),
-- Water Utilities
('4941', 'Utilities', 'Water Utilities', 'high', 'Water supply - AWK'),

-- =========================================================================
-- ENERGY
-- =========================================================================
-- Oil & Gas E&P
('1311', 'Energy', 'Oil & Gas E&P', 'high', 'Crude petroleum & natural gas - EOG, DVN, FANG, OXY'),
('6792', 'Energy', 'Oil & Gas E&P', 'high', 'Oil royalty traders - TPL mineral rights'),
-- Oil & Gas Refining
('2911', 'Energy', 'Oil & Gas Refining', 'high', 'Petroleum refining - XOM, CVX, COP, VLO, MPC'),
-- Oil & Gas Equipment & Services
('1389', 'Energy', 'Oil & Gas Equipment', 'high', 'Oil & gas field services - SLB, HAL'),
('3533', 'Energy', 'Oil & Gas Equipment', 'high', 'Oil & gas field machinery - BKR'),
-- Midstream
('4610', 'Energy', 'Oil & Gas Midstream', 'high', 'Pipelines (no natural gas) - PAA, PAGP'),

-- =========================================================================
-- MATERIALS
-- =========================================================================
-- Chemicals (28xx)
('2821', 'Materials', 'Chemicals', 'high', 'Plastics & resins - DOW, DD'),
('2810', 'Materials', 'Industrial Gases', 'high', 'Industrial inorganic chemicals - APD, LIN'),
('2860', 'Materials', 'Specialty Chemicals', 'high', 'Industrial organic chemicals - IFF, LYB'),
('2870', 'Materials', 'Fertilizers', 'high', 'Agricultural chemicals - CF, MOS'),
('2800', 'Materials', 'Chemicals', 'high', 'Chemicals & allied products - BCPC'),
('2851', 'Materials', 'Specialty Chemicals', 'high', 'Paints, varnishes - PPG'),
-- Metals & Mining (1xxx, 33xx)
('1000', 'Materials', 'Metals & Mining', 'high', 'Metal mining - FCX copper'),
('1040', 'Materials', 'Gold', 'high', 'Gold and silver ores - NEM'),
('1400', 'Materials', 'Construction Materials', 'high', 'Mining of nonmetallic minerals - MLM, VMC aggregates'),
('3312', 'Materials', 'Steel', 'high', 'Steel works & blast furnaces - NUE, STLD'),
('3334', 'Materials', 'Aluminum', 'high', 'Primary aluminum production - CENX'),
('3350', 'Materials', 'Metals & Mining', 'high', 'Rolling & extruding nonferrous metals - HWM aerospace metals'),
('3241', 'Materials', 'Construction Materials', 'high', 'Cement, hydraulic - CRH'),
('6795', 'Materials', 'Gold', 'high', 'Mineral royalty traders - RGLD, gold streaming'),
-- Containers & Packaging
('2650', 'Materials', 'Containers & Packaging', 'high', 'Paperboard containers & boxes - PKG'),
('2670', 'Materials', 'Containers & Packaging', 'high', 'Converted paper products - AVY, KMB'),
('2673', 'Materials', 'Containers & Packaging', 'high', 'Plastics, foil & coated paper bags - REYN'),
('3411', 'Materials', 'Containers & Packaging', 'high', 'Metal cans - BALL'),
('3089', 'Materials', 'Containers & Packaging', 'high', 'Plastics products NEC - ENTG semiconductor materials'),
('3990', 'Materials', 'Containers & Packaging', 'high', 'Miscellaneous manufacturing - AMCR packaging'),
-- Paper & Forest Products
('2621', 'Materials', 'Paper Products', 'high', 'Paper mills - IP'),
('2421', 'Materials', 'Forest Products', 'high', 'Sawmills & planing mills - UFPI'),
-- Agriculture (special)
('0100', 'Materials', 'Agricultural Inputs', 'high', 'Agricultural production - CTVA seeds & crop protection'),

-- =========================================================================
-- INDUSTRIALS
-- =========================================================================
-- Aerospace & Defense (376x, 372x, 373x, 381x)
('3760', 'Industrials', 'Aerospace & Defense', 'high', 'Guided missiles & space vehicles - LMT, RKLB'),
('3812', 'Industrials', 'Aerospace & Defense', 'high', 'Search, detection, navigation systems - NOC, LHX, TDY'),
('3721', 'Industrials', 'Aerospace & Defense', 'high', 'Aircraft - BA, AVAV'),
('3720', 'Industrials', 'Aerospace & Defense', 'high', 'Aircraft & parts - TXT'),
('3724', 'Industrials', 'Aerospace & Defense', 'high', 'Aircraft engines & engine parts - RTX, HON'),
('3728', 'Industrials', 'Aerospace & Defense', 'high', 'Aircraft parts & auxiliary equipment - TDG'),
('3730', 'Industrials', 'Aerospace & Defense', 'high', 'Ship & boat building - GD, HII'),
('3480', 'Industrials', 'Aerospace & Defense', 'high', 'Ordnance & accessories - AXON'),
-- Industrial Machinery (35xx)
('3560', 'Industrials', 'Industrial Machinery', 'high', 'General industrial machinery - IR, ITW'),
('3561', 'Industrials', 'Industrial Machinery', 'high', 'Pumps & pumping equipment - XYL, IEX'),
('3569', 'Industrials', 'Industrial Machinery', 'high', 'General industrial machinery NEC - NDSN'),
('3510', 'Industrials', 'Industrial Machinery', 'high', 'Engines & turbines - CMI'),
('3523', 'Industrials', 'Agricultural Machinery', 'high', 'Farm machinery & equipment - DE'),
('3530', 'Industrials', 'Construction Machinery', 'high', 'Construction & mining machinery - DOV'),
('3531', 'Industrials', 'Construction Machinery', 'high', 'Construction machinery & equipment - CAT'),
('3540', 'Industrials', 'Industrial Machinery', 'high', 'Metalworking machinery - LECO'),
('3550', 'Industrials', 'Industrial Machinery', 'high', 'Special industry machinery - PNR'),
('3580', 'Industrials', 'Industrial Machinery', 'high', 'Refrigeration & service industry machinery - MIDD'),
('3590', 'Industrials', 'Electrical Equipment', 'high', 'Misc industrial machinery - ETN'),
('3743', 'Industrials', 'Industrial Machinery', 'high', 'Railroad equipment - WAB'),
('3490', 'Industrials', 'Industrial Machinery', 'high', 'Miscellaneous fabricated metal products - PH'),
('3420', 'Industrials', 'Industrial Machinery', 'high', 'Cutlery, handtools & hardware - SNA, SWK'),
-- Electrical Equipment (36xx)
('3600', 'Industrials', 'Electrical Equipment', 'high', 'Electronic & electrical equipment - GE, EMR'),
('3620', 'Industrials', 'Electrical Equipment', 'high', 'Electrical industrial apparatus - WWD'),
('3621', 'Industrials', 'Electrical Equipment', 'high', 'Motors & generators - GNRC, FELE'),
('3613', 'Industrials', 'Electrical Equipment', 'high', 'Switchgear & switchboard apparatus - POWL'),
('3690', 'Industrials', 'Electrical Equipment', 'medium', 'Misc electrical machinery - includes clean energy RUN, QS'),
-- Building Products (358x, 343x)
('3585', 'Industrials', 'Building Products', 'high', 'HVAC equipment - CARR, JCI, LII'),
('3430', 'Industrials', 'Building Products', 'high', 'Heating equipment & plumbing fixtures - MAS'),
('3822', 'Industrials', 'Building Products', 'high', 'Auto controls for environments - TT (Trane)'),
-- Test & Measurement (382x, 383x)
('3823', 'Industrials', 'Industrial Instruments', 'medium', 'Industrial instruments - DHR, ROP, FTV (some IT-adjacent)'),
('3825', 'Industrials', 'Electronic Equipment', 'medium', 'Instruments for testing electricity - TER (semiconductor test)'),
('3829', 'Industrials', 'Industrial Instruments', 'medium', 'Measuring & controlling devices - TMO, ROK, TRMB'),
-- Transportation - Rail
('4011', 'Industrials', 'Railroads', 'high', 'Railroads - UNP, CSX, NSC'),
-- Transportation - Air Freight & Logistics
('4210', 'Industrials', 'Air Freight & Logistics', 'high', 'Trucking & courier services - UPS'),
('4213', 'Industrials', 'Ground Transportation', 'high', 'Trucking (no local) - ODFL, SAIA, JBHT'),
('4513', 'Industrials', 'Air Freight & Logistics', 'high', 'Air courier services - FDX'),
('4731', 'Industrials', 'Air Freight & Logistics', 'high', 'Freight arrangement - CHRW, EXPD'),
-- Transportation - Airlines
('4512', 'Industrials', 'Airlines', 'high', 'Air transportation, scheduled - DAL, UAL, LUV, AAL'),
-- Construction & Engineering (15xx, 16xx, 17xx)
('1731', 'Industrials', 'Construction & Engineering', 'high', 'Electrical work - EME, PWR'),
('1600', 'Industrials', 'Construction & Engineering', 'high', 'Heavy construction - J (Jacobs)'),
-- Professional Services
('7320', 'Industrials', 'Research & Consulting', 'high', 'Consumer credit reporting - EFX, MCO, SPGI (rating agencies)'),
('8700', 'Industrials', 'Professional Services', 'high', 'Engineering, accounting, research - PAYX'),
('8711', 'Industrials', 'Construction & Engineering', 'high', 'Engineering services - TTEK'),
-- Trading Companies & Distributors
('5000', 'Industrials', 'Trading Companies', 'high', 'Wholesale durable goods - GWW'),
('5065', 'Industrials', 'Trading Companies', 'medium', 'Wholesale electronic parts - AVT, TEL'),
-- Commercial Services
('7350', 'Industrials', 'Trading Companies', 'high', 'Equipment rental & leasing - FTAI aviation leasing'),
('7359', 'Industrials', 'Trading Companies', 'high', 'Equipment rental & leasing NEC - URI'),
('7381', 'Industrials', 'Commercial Services', 'high', 'Detective, guard services - ALLE (security products)'),
-- Waste Management
('4953', 'Industrials', 'Waste Management', 'high', 'Refuse systems - WM, RSG, CWST'),

-- =========================================================================
-- CONSUMER DISCRETIONARY
-- =========================================================================
-- Retail - General (533x)
('5331', 'Consumer Discretionary', 'Broadline Retail', 'high', 'Variety stores - WMT, TGT, COST, DG, DLTR'),
('5961', 'Consumer Discretionary', 'Internet Retail', 'high', 'Catalog & mail-order - AMZN, CDW'),
-- Retail - Home Improvement
('5211', 'Consumer Discretionary', 'Home Improvement', 'high', 'Lumber & building materials - HD, LOW, BLDR'),
('5200', 'Consumer Discretionary', 'Home Improvement', 'high', 'Building materials retail - FAST, SHW, TSCO'),
-- Retail - Specialty
('5651', 'Consumer Discretionary', 'Apparel Retail', 'high', 'Family clothing stores - TJX, ROST'),
('5700', 'Consumer Discretionary', 'Home Furnishing Retail', 'high', 'Home furniture & equipment - WSM'),
('5731', 'Consumer Discretionary', 'Computer & Electronics Retail', 'high', 'Radio, TV & electronics retail - BBY'),
('5900', 'Consumer Discretionary', 'Specialty Retail', 'high', 'Miscellaneous retail - FCFS'),
('5990', 'Consumer Discretionary', 'Specialty Retail', 'high', 'Retail stores NEC - ULTA'),
-- Retail - Auto
('5500', 'Consumer Discretionary', 'Automotive Retail', 'high', 'Auto dealers & gas stations - CPRT, CVNA, CASY'),
('5531', 'Consumer Discretionary', 'Automotive Retail', 'high', 'Auto & home supply stores - AZO, ORLY'),
('5090', 'Consumer Discretionary', 'Specialty Retail', 'high', 'Wholesale misc durable goods - POOL'),
-- Automobiles (371x)
('3711', 'Consumer Discretionary', 'Automobiles', 'high', 'Motor vehicles - TSLA, F, GM'),
('3714', 'Consumer Discretionary', 'Auto Parts', 'high', 'Motor vehicle parts - APTV, GNTX'),
-- Hotels, Restaurants & Leisure
('7011', 'Consumer Discretionary', 'Hotels & Resorts', 'high', 'Hotels & motels - MAR, HLT, also casinos LVS, WYNN, MGM'),
('5812', 'Consumer Discretionary', 'Restaurants', 'high', 'Eating places - MCD, CMG, TXRH, DRI, YUM'),
('5810', 'Consumer Discretionary', 'Restaurants', 'high', 'Eating & drinking places - SBUX'),
('7948', 'Consumer Discretionary', 'Casinos & Gaming', 'high', 'Racing, including track operation - CHDN'),
('7990', 'Consumer Discretionary', 'Casinos & Gaming', 'medium', 'Misc amusement & recreation - DIS (also Comm Svc), DKNG'),
-- Cruise Lines & Travel
('4400', 'Consumer Discretionary', 'Hotels & Resorts', 'high', 'Water transportation - CCL, RCL, NCLH cruise lines'),
('4700', 'Consumer Discretionary', 'Hotels & Resorts', 'high', 'Transportation services - BKNG, EXPE travel booking'),
('7510', 'Consumer Discretionary', 'Rental & Leasing', 'high', 'Auto rental & leasing - CAR'),
('7340', 'Consumer Discretionary', 'Hotels & Resorts', 'medium', 'Services to dwellings - ABNB (vacation rentals), ROL (Industrials)'),
-- Homebuilders
('1531', 'Consumer Discretionary', 'Homebuilding', 'high', 'Operative builders - DHI, NVR, PHM homebuilders'),
('1520', 'Consumer Discretionary', 'Homebuilding', 'high', 'Residential building contractors - LEN'),
-- Apparel & Footwear
('2300', 'Consumer Discretionary', 'Apparel', 'high', 'Apparel & other finished products - LULU'),
('2320', 'Consumer Discretionary', 'Apparel', 'medium', 'Mens & boys furnishings - RL, but CTAS is Industrials'),
('3021', 'Consumer Discretionary', 'Footwear', 'high', 'Rubber & plastics footwear - NKE, DECK, CROX'),
('3100', 'Consumer Discretionary', 'Apparel & Luxury', 'high', 'Leather products - TPR (Coach, Kate Spade)'),
-- Household Durables
('3630', 'Consumer Discretionary', 'Household Durables', 'high', 'Household appliances - AOS'),
-- Leisure Products
('3942', 'Consumer Discretionary', 'Leisure Products', 'high', 'Dolls & stuffed toys - MAT'),
('3944', 'Consumer Discretionary', 'Leisure Products', 'high', 'Games, toys & childrens vehicles - HAS'),
-- Education
('8200', 'Consumer Discretionary', 'Education Services', 'high', 'Educational services - LOPE, LAUR'),
-- Auto Parts Wholesale
('5013', 'Consumer Discretionary', 'Auto Parts', 'high', 'Wholesale motor vehicle supplies - GPC'),

-- =========================================================================
-- CONSUMER STAPLES
-- =========================================================================
-- Beverages (208x)
('2080', 'Consumer Staples', 'Beverages', 'high', 'Beverages - KO, PEP, KDP, STZ'),
('2086', 'Consumer Staples', 'Soft Drinks', 'high', 'Bottled & canned soft drinks - MNST, CELH'),
('2082', 'Consumer Staples', 'Brewers', 'high', 'Malt beverages - TAP'),
-- Food Products (20xx)
('2000', 'Consumer Staples', 'Packaged Foods', 'high', 'Food and kindred products - CAG, CPB, MDLZ'),
('2011', 'Consumer Staples', 'Packaged Foods', 'high', 'Meat packing plants - HRL'),
('2015', 'Consumer Staples', 'Packaged Foods', 'high', 'Poultry processing - TSN, PPC'),
('2030', 'Consumer Staples', 'Packaged Foods', 'high', 'Canned, frozen & preserved food - KHC, LW'),
('2033', 'Consumer Staples', 'Packaged Foods', 'high', 'Canned fruits, vegetables - SJM'),
('2040', 'Consumer Staples', 'Packaged Foods', 'high', 'Grain mill products - GIS'),
('2060', 'Consumer Staples', 'Packaged Foods', 'high', 'Sugar & confectionery - HSY'),
('2070', 'Consumer Staples', 'Agricultural Products', 'high', 'Fats & oils - ADM, BG'),
('2090', 'Consumer Staples', 'Packaged Foods', 'high', 'Misc food preparations - MKC'),
-- Tobacco
('2111', 'Consumer Staples', 'Tobacco', 'high', 'Cigarettes - PM, MO'),
-- Household & Personal Products (284x)
('2840', 'Consumer Staples', 'Household Products', 'high', 'Soap, detergents, cleaning - PG, ECL, CHD'),
('2842', 'Consumer Staples', 'Household Products', 'high', 'Specialty cleaning preparations - CLX'),
('2844', 'Consumer Staples', 'Personal Products', 'high', 'Perfumes, cosmetics - EL, CL'),
-- Food Retail & Distribution
('5411', 'Consumer Staples', 'Food Retail', 'high', 'Grocery stores - KR, SFM'),
('5140', 'Consumer Staples', 'Food Distributors', 'high', 'Wholesale groceries - SYY, also DPZ (restaurants)'),
-- Drug Retail
('5912', 'Consumer Staples', 'Drug Retail', 'high', 'Drug stores - CVS')

ON CONFLICT (sic_code) DO UPDATE SET
    gics_sector = EXCLUDED.gics_sector,
    gics_industry = EXCLUDED.gics_industry,
    confidence = EXCLUDED.confidence,
    notes = EXCLUDED.notes,
    updated_at = CURRENT_TIMESTAMP;
