"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 327 / 400
================================================================================
- Group: ThirdTest_pincode_group_33_parts_321_to_330
- Assigned PIN Codes: 48 (Range: 732139 to 734002)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_327.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_327.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_327"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-327] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "732139",
  "732140",
  "732141",
  "732142",
  "732144",
  "732201",
  "732202",
  "732203",
  "732204",
  "732205",
  "732206",
  "732207",
  "732208",
  "732209",
  "732210",
  "732215",
  "732216",
  "733101",
  "733102",
  "733103",
  "733121",
  "733123",
  "733124",
  "733125",
  "733126",
  "733127",
  "733128",
  "733129",
  "733130",
  "733132",
  "733133",
  "733134",
  "733140",
  "733141",
  "733142",
  "733143",
  "733145",
  "733156",
  "733158",
  "733201",
  "733202",
  "733207",
  "733208",
  "733209",
  "733210",
  "733215",
  "734001",
  "734002"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "732139": {
    "pincode": "732139",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Samsi SO",
      "Amarsinghi BO",
      "Baldiapukur BO",
      "Batna BO",
      "Bhado BO",
      "Chandmoni BO",
      "Chorolmoni BO",
      "Damaipur BO",
      "Gangadevi BO",
      "Jitarpur BO",
      "Kandaran BO",
      "Karbona BO",
      "Kashimpur BO",
      "Sonarai BO",
      "Sripur BO"
    ]
  },
  "732140": {
    "pincode": "732140",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Tulsihatta SO",
      "Bangrua BO",
      "Bhatolchandipur BO",
      "Bishanpur BO",
      "Boroi BO",
      "Chandipur BO",
      "Jabra BO",
      "Kushida BO",
      "Paro BO",
      "Ramsimul BO"
    ]
  },
  "732141": {
    "pincode": "732141",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Narayanpur SO Malda",
      "Jatradanga BO",
      "Popra BO"
    ]
  },
  "732142": {
    "pincode": "732142",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Mangalbari SO",
      "Bachamari BO",
      "Madhaipur BO",
      "Nageswarpur BO",
      "Sahapur BO"
    ]
  },
  "732144": {
    "pincode": "732144",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Kotwali SO Malda"
    ]
  },
  "732201": {
    "pincode": "732201",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Kaliachak SO",
      "Alinagar BO",
      "Alipur BO",
      "Bahadurpur BO",
      "Baliadanga BO",
      "Bholaichak BO",
      "Charianantapur BO",
      "Dakshin Lakshmipur BO",
      "Dallugram BO",
      "Duisatabighi BO",
      "Golapganj BO",
      "Haruchak BO",
      "Jotparam BO",
      "Joyenpur BO",
      "Khaschandpur BO",
      "Nabinagar BO",
      "Nayagram BO",
      "Purba Bahadurpur BO",
      "Rajnagar BO",
      "Ramnagar BO",
      "Sabdalpur BO",
      "Sahabazpur BO",
      "Shashani BO",
      "Shersahi BO",
      "Sukdevpur BO",
      "Sultanganj BO",
      "Uttar Dariapur BO"
    ]
  },
  "732202": {
    "pincode": "732202",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Manikchak SO",
      "Chowki Mirdadpur BO",
      "Dallutola BO",
      "Dharampur BO",
      "Enayetpur BO",
      "Janakiramtola BO",
      "Mohana BO",
      "Purbasaidpur BO",
      "Rahimpur BO"
    ]
  },
  "732203": {
    "pincode": "732203",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Mathurapur SO Malda",
      "Bakdukra Anantalalpur BO",
      "Bhutni BO",
      "Khairtola BO",
      "Lalbathani BO",
      "Nazirpur BO",
      "Noaborar Jaigir BO",
      "Nurpur BO",
      "Sukdevtola BO",
      "Suksena BO",
      "Uttar Chandipur BO"
    ]
  },
  "732204": {
    "pincode": "732204",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Paranpur SO",
      "Brahmangram BO",
      "Ekborna BO",
      "Goraksha BO",
      "Khoilsona BO",
      "Mirjatpur BO",
      "Araidanga BO",
      "Pukhuria BO"
    ]
  },
  "732205": {
    "pincode": "732205",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Ratua SO",
      "Baharal BO",
      "Bahirkap BO",
      "Balupur BO",
      "Banikantatala BO",
      "Bilaimari BO",
      "Debipur BO",
      "Kahala BO",
      "Kamalpur BO",
      "Laskarpur BO"
    ]
  },
  "732206": {
    "pincode": "732206",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Sujapur SO",
      "Bakharpur BO",
      "Baluachara BO",
      "Bamangram BO",
      "Chaspara BO",
      "Chhoto Sujapur BO",
      "Fatehkhani BO",
      "Gayeshbari BO",
      "Jalalpur BO",
      "Jamirghata Sarkarpara BO",
      "Madhugaht Filature Estate BO",
      "Mosimpur BO",
      "Sherpur BO",
      "Jadupur BO",
      "Sayedpur BO"
    ]
  },
  "732207": {
    "pincode": "732207",
    "circle": "West bengal circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Jagannathpur Rathbari BO",
      "Jitnagar BO",
      "Jotgopal Kagmari BO",
      "Jugaltola BO",
      "Khaskol Chandipur BO",
      "Meherapur BO",
      "Panchanandapur BO",
      "Sripur Colony BO",
      "Uttar Lakshmipur BO",
      "Mothabari SO",
      "Bangitola BO",
      "Birampur BO",
      "Damodartola BO",
      "Debipur Achintala BO",
      "Gangaprasad BO",
      "Hamidpur BO"
    ]
  },
  "732208": {
    "pincode": "732208",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Amrity SO",
      "Atgama BO",
      "Koklamari BO",
      "Madapur BO",
      "Nagharia BO",
      "Niamatpur BO",
      "Phulbaria BO",
      "Sattari BO"
    ]
  },
  "732209": {
    "pincode": "732209",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Sovanagar SO",
      "Bhabanipur BO",
      "Gopalpur BO",
      "KB Jhowbona BO",
      "Pirpur BO",
      "Sahabattola BO",
      "Milki BO"
    ]
  },
  "732210": {
    "pincode": "732210",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Baishnabnagar SO",
      "Bedrabad BO",
      "Bhgabanpur BO",
      "Chakbahadurpur BO",
      "Char Sujapur Mandai BO",
      "Krishnapur BO",
      "Lakshmipur BO",
      "Nandalalpur BO"
    ]
  },
  "732215": {
    "pincode": "732215",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Pubarun SO"
    ]
  },
  "732216": {
    "pincode": "732216",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Mahadipur SO",
      "Akandabaria BO",
      "Jaluabadhal Mallikpara BO",
      "Kadamtala BO",
      "Nawada BO",
      "South Kadamtala BO",
      "Srirampur BO",
      "Umakantatola BO",
      "Uttar Mahadipur BO"
    ]
  },
  "733101": {
    "pincode": "733101",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Balurghat HO",
      "Balurghat Market SO",
      "Chakbhabani SO",
      "Khadimpur SO",
      "Narayanpur SO South Dinajpur",
      "School Para SO"
    ]
  },
  "733102": {
    "pincode": "733102",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Chakvrigu SO",
      "Barakashipur BO",
      "Bharila BO",
      "Bhatra BO",
      "Dakra BO",
      "Gofanagar BO",
      "Jalghar BO",
      "Khaspur BO",
      "Kuaran BO",
      "Radhanagar BO",
      "Rajua BO"
    ]
  },
  "733103": {
    "pincode": "733103",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Beltala Park SO",
      "Amrail BO",
      "Amritakhanda Hat BO",
      "Bedoypur BO",
      "Bijoyshree BO",
      "Birohini BO",
      "Dakshin Shibrampur BO",
      "Khidirpur BO",
      "Nunail BO",
      "Sarangram BO",
      "BaraRaghunathpur SO",
      "District School Board SO South Dinajpur"
    ]
  },
  "733121": {
    "pincode": "733121",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Buniadpur SO",
      "Badalpur BO",
      "Banshihari BO",
      "Biswanathpur BO",
      "Cheragipara BO",
      "Dikul BO",
      "Durgapur BO",
      "Joredighi BO",
      "Kalikamora BO",
      "Karai BO",
      "Karkha BO",
      "Kushkari BO",
      "Mahipal BO",
      "Nilgambhir BO",
      "Sihole BO",
      "Sudarshannagar BO"
    ]
  },
  "733123": {
    "pincode": "733123",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Debinagar SO",
      "Bariol BO",
      "Birghai BO",
      "Gorahar BO",
      "Hatia BO",
      "Ital BO",
      "Keotal BO",
      "Paschim Manoharpur BO",
      "Rupahar BO",
      "Tenohari BO"
    ]
  },
  "733124": {
    "pincode": "733124",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Gangarampur SO",
      "Belbari BO",
      "Champatali BO",
      "Chenchra BO",
      "Dobakhokshan BO",
      "Jahangirpur BO",
      "Jalalpur BO",
      "Kaldighi BO",
      "Kantabari BO",
      "Laxmitola BO",
      "Madnabati BO",
      "Nalagola BO",
      "Nandanpur BO",
      "Narai BO",
      "Purba Mollapara BO",
      "Raghabpur BO",
      "Rajibpur BO",
      "Ratanpur BO",
      "Sarbamangala BO",
      "Sukdebpur BO"
    ]
  },
  "733125": {
    "pincode": "733125",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Harirampur SO",
      "Bagichapur BO",
      "Bairhatta BO",
      "Balihara BO",
      "Bartakigram BO",
      "Danagram BO",
      "Daulatpur South DinajpurBO",
      "Kanaipur BO",
      "Maliandighi BO",
      "Pundari BO",
      "Saiyadpur Nayapara BO",
      "Sandhiya BO",
      "Singadaha BO"
    ]
  },
  "733126": {
    "pincode": "733126",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Hili SO",
      "Fatepur BO",
      "Trimohini BO",
      "Hili Aftair BO"
    ]
  },
  "733127": {
    "pincode": "733127",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Tapan SO",
      "Ajmatpur BO",
      "Balapur BO",
      "Banial BO",
      "Chakbaliram BO",
      "Daralhat BO",
      "Holidana BO",
      "Kamdebbati BO",
      "Karaichenchra BO",
      "Laskarhat BO",
      "Mahadebpur BO",
      "Patkola BO",
      "Ramchandrapur BO",
      "Telighata BO",
      "Tilon BO"
    ]
  },
  "733128": {
    "pincode": "733128",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Itahar SO",
      "Baidara BO",
      "Baragarm BO",
      "Chavote BO",
      "Chhayghara BO",
      "Churaman BO",
      "Gulandar BO",
      "Joyhat BO",
      "Kalaibari BO",
      "Kapasia BO",
      "Marnai BO",
      "Nomunia BO",
      "Patirajpur BO",
      "Radhanagar BO",
      "Tilna BO"
    ]
  },
  "733129": {
    "pincode": "733129",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Kaliyaganj SO",
      "Baghan BO",
      "Bhanail BO",
      "Bhelai BO",
      "Dalimgaon BO",
      "Dhankoil Hat BO",
      "Faridpur BO",
      "Krishnabati BO",
      "Kunor BO",
      "Madhabpur BO",
      "Malgaon BO",
      "Manoharpur BO",
      "Mudafat BO",
      "Puria BO",
      "Radhikapur BO",
      "Raghunathpur BO",
      "Tamchari Mathbari BO",
      "Tarangapur BO",
      "Tungail Bilpara BO",
      "Kaliyaganj Collegepara SO",
      "Mahendraganj SO North Dinajpur",
      "Akhanagar BO"
    ]
  },
  "733130": {
    "pincode": "733130",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Karnojora SO",
      "Dadhikotbari BO",
      "Hemtabad BO",
      "Mirual BO"
    ]
  },
  "733132": {
    "pincode": "733132",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Kushmandi SO",
      "Aminpur BO",
      "Arajipanishalahat BO",
      "Basoil BO",
      "Berail BO",
      "Chousa BO",
      "Dehaband BO",
      "Fatepur Hat BO",
      "Majhihar BO",
      "Manikore BO",
      "Nahit BO",
      "Parameshwarpur BO",
      "Sahapur Jharbari BO",
      "Sarala BO",
      "Sibkrishnapur BO",
      "Usha Haranhat BO",
      "Uttar Karanji BO",
      "Uttar Para BO"
    ]
  },
  "733133": {
    "pincode": "733133",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Patiram SO",
      "Batun BO",
      "Beltara BO",
      "Gobindapur BO",
      "Khanpur BO",
      "Nazirpur BO",
      "Radhakrishnapur BO",
      "Sayedpur BO"
    ]
  },
  "733134": {
    "pincode": "733134",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Raiganj SO",
      "Baharail BO",
      "Bahin BO",
      "Bamangram BO",
      "Bangalbari BO",
      "Bhitiar BO",
      "Bhogram BO",
      "Bilashpur BO",
      "Harigram BO",
      "Khalshi BO",
      "Lohanda BO",
      "Madhupur Barduari BO",
      "Panishalahat BO",
      "Raipur BO",
      "Samaspur BO",
      "Sasan BO",
      "Subhashganj BO",
      "Taherpur BO",
      "Industrial Market SO",
      "Mohanbati SO",
      "Raiganj Bandar SO",
      "Raiganj Town SO",
      "Rashbehari Market SO",
      "Sudarshanpur SO"
    ]
  },
  "733140": {
    "pincode": "733140",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Rampur SO South Dinajpur",
      "Bandighi BO",
      "Deor BO",
      "Fulbari BO",
      "Janchi BO",
      "Mahanaj BO",
      "Panchagram BO",
      "Pransagar BO",
      "Ramkrishnapur BO"
    ]
  },
  "733141": {
    "pincode": "733141",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Gopalganj SO",
      "Angina BO",
      "Ashokegram BO",
      "Bhagabatipur BO",
      "Bholanathpur BO",
      "Chandganj BO",
      "Churail Krishnapur BO",
      "Dangarhat BO",
      "Debipur BO",
      "Jakhirpur BO",
      "Kumarganj BO",
      "Kuraha BO",
      "Mahipur BO",
      "Narayanpur BO",
      "Panitara BO",
      "Safanagar BO",
      "Samjhia BO"
    ]
  },
  "733142": {
    "pincode": "733142",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Nayabazar SO South Dinajpur",
      "Basuria BO",
      "Bhikahar BO",
      "Chaksukdebpur BO",
      "Gurail BO",
      "Joypur BO",
      "Kardah BO",
      "Kupadaha BO",
      "Manohali BO"
    ]
  },
  "733143": {
    "pincode": "733143",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Bhupalpur SO",
      "Bekidanga BO",
      "Bhagnail BO",
      "Durlavpur BO",
      "Hasua Samadhimath BO",
      "Hatgachhi BO",
      "Kamlai BO",
      "Paraharipur BO",
      "Parbatipur BO",
      "Parergram BO",
      "Sonapur BO"
    ]
  },
  "733145": {
    "pincode": "733145",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Teor SO",
      "Chingishpur BO",
      "Gopalbati BO",
      "Jamalpur BO",
      "Kamarpara BO",
      "Powrahar BO",
      "Thakurpurahat BO"
    ]
  },
  "733156": {
    "pincode": "733156",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Bindole SO",
      "Basian BO",
      "Bharatpur BO",
      "Bhatolhat BO",
      "Chainagar BO",
      "Kantore BO",
      "Khokshan BO",
      "Laxmonia BO",
      "Maharajahat BO",
      "Panchbhaya BO",
      "Runia BO"
    ]
  },
  "733158": {
    "pincode": "733158",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Atrai SO",
      "Baul BO",
      "Bolla BO"
    ]
  },
  "733201": {
    "pincode": "733201",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Dalkhola SO",
      "Andharia BO",
      "Bazargaon BO",
      "Jagadishpur BO",
      "Khurka BO",
      "Lalganj BO",
      "Magnavita BO",
      "Maheshpur BO",
      "Patnaur BO",
      "Rashokhowahat BO",
      "Shikarpur BO",
      "Sripur BO"
    ]
  },
  "733202": {
    "pincode": "733202",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Islampur SO North Dinajpur",
      "Amaljhari BO",
      "BurhijagirBO",
      "Dhantala BO",
      "Dimrulla BO",
      "Dulalibhita BO",
      "Gunjaria Bazar BO",
      "Haptiagachh BO",
      "Jagtagaon BO",
      "Kalanagin BO",
      "Khunti BO",
      "Kodaldaha BO",
      "Larukhoa Gobindapur BO",
      "Matikundahat BO",
      "Panchdimthi BO",
      "Sonapur Hat BO",
      "Srikrishnapur Colony BO",
      "State Farm Colony BO",
      "Islampur Court SO"
    ]
  },
  "733207": {
    "pincode": "733207",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Ramganj SO North Dinajpur",
      "Asharubasti BO",
      "Bhaispitta BO",
      "Chandanidanga BO",
      "Chargharia BO",
      "Chopra BO",
      "Daspara BO",
      "Debijhora BO",
      "Dighabanahat BO",
      "Gendagachh BO",
      "Gharugachh BO",
      "Kaimari BO",
      "Khunia BO",
      "Kuchilagoan BO",
      "Lakshipur BO",
      "Manikpur BO",
      "Molani BO",
      "Narayanpur BO",
      "Sujali BO"
    ]
  },
  "733208": {
    "pincode": "733208",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Panjipara SO",
      "Amalia BO",
      "Bidyanandapur BO",
      "Bhebra BO",
      "Chakulia Hat BO",
      "Chapore BO",
      "Goalgoan BO",
      "Gohara BO",
      "Ikarchala BO",
      "Kaniabhita BO",
      "Khikirtola BO",
      "Nando BO",
      "Pokhoria BO",
      "Sakuntala BO",
      "Tarial BO",
      "Thakurbari BO"
    ]
  },
  "733209": {
    "pincode": "733209",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Kanki SO",
      "Asuragarh BO",
      "Bagdob BO",
      "Bharna BO",
      "Galia BO",
      "Hassan BO",
      "Hatwar BO",
      "Majlishpur BO",
      "Monora BO",
      "Nizampur BO",
      "Sayedpur Bhamantali BO",
      "Simalia BO",
      "Surjapur BO"
    ]
  },
  "733210": {
    "pincode": "733210",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Goalpokher SO",
      "Bahar BO",
      "Baramajlishpur BO",
      "Barbilla BO",
      "Betna BO",
      "Bhabanbari BO",
      "Bhendabari BO",
      "Debiganj BO",
      "Dehar BO",
      "Gendabari BO",
      "Goagaon BO",
      "Goalin BO",
      "Goti BO",
      "Hatkholasahapur BO",
      "Jharbari BO",
      "Khagore BO",
      "Kichaktola BO",
      "Lalkuri BO",
      "Malkunda BO",
      "Sahapur BO",
      "Solpara BO"
    ]
  },
  "733215": {
    "pincode": "733215",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Dinajpur Division",
    "offices": [
      "Karandighi SO",
      "Altapur BO",
      "Doarin BO",
      "Domohana BO",
      "Kamartore BO",
      "Madargachhi BO",
      "Sawdhan BO",
      "Tungidighi BO"
    ]
  },
  "734001": {
    "pincode": "734001",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Darjeeling Division",
    "offices": [
      "Siliguri HO",
      "Mahananda Bridge SO",
      "Rath Khola SO",
      "Saktigarh SO Jalpaiguri",
      "Sevoke Road SO",
      "Siliguri Junction SO",
      "Siliguri New Market SO",
      "Siliguri Road SO",
      "Subashpally SO",
      "Vivekanandapally SO"
    ]
  },
  "734002": {
    "pincode": "734002",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Darjeeling Division",
    "offices": [
      "Salbari SO"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
