"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 326 / 400
================================================================================
- Group: ThirdTest_pincode_group_33_parts_321_to_330
- Assigned PIN Codes: 48 (Range: 731126 to 732138)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_326.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_326.csv & .json
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

PART_ID = "part_326"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-326] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "731126",
  "731127",
  "731129",
  "731130",
  "731132",
  "731133",
  "731201",
  "731202",
  "731204",
  "731213",
  "731214",
  "731215",
  "731216",
  "731218",
  "731219",
  "731220",
  "731221",
  "731222",
  "731223",
  "731224",
  "731233",
  "731234",
  "731235",
  "731236",
  "731237",
  "731238",
  "731240",
  "731241",
  "731242",
  "731243",
  "731244",
  "731245",
  "731301",
  "731302",
  "731303",
  "731304",
  "732101",
  "732102",
  "732103",
  "732121",
  "732122",
  "732123",
  "732124",
  "732125",
  "732126",
  "732127",
  "732128",
  "732138"
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
  "731126": {
    "pincode": "731126",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bhabanipur BO",
      "Ganeshpur BO",
      "Ghatdurlavpur BO",
      "Gohaliara BO",
      "Madhaipur BO",
      "Nagari BO",
      "Paruliahazrapur BO",
      "Patadanga BO",
      "Patharchapuri BO",
      "Rajganj BO",
      "Rautara BO",
      "Sajina BO",
      "Tabadumra BO",
      "Tantipara BO",
      "Karidhya SO"
    ]
  },
  "731127": {
    "pincode": "731127",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "MdBazar SO",
      "Amjolpahari BO",
      "Baidyanathpur BO",
      "Baliharpur BO",
      "Bishnupurkulkuri BO",
      "Charicha BO",
      "Debagramchuramali BO",
      "Kapista BO",
      "Nimdaspur BO",
      "Rampur BO",
      "Sarenda BO",
      "Sehelaraipur BO",
      "Seherakuri BO"
    ]
  },
  "731129": {
    "pincode": "731129",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Damdama BO",
      "Dholtikuri BO",
      "Gadadharpur BO",
      "Ikra BO",
      "Majhigram BO",
      "Purundarpur SO"
    ]
  },
  "731130": {
    "pincode": "731130",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Aligarh BO",
      "Joypur BO",
      "Kundira BO",
      "Kurulmetia BO",
      "Lauberia BO",
      "Laujore BO",
      "Muktipur BO",
      "Ranigram BO",
      "Rajnagar SO Birbhum"
    ]
  },
  "731132": {
    "pincode": "731132",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Deucha BO",
      "Harinsingha BO",
      "Kabilpur BO",
      "Nischintapur Jagatpur BO",
      "Puranagram BO",
      "MdBazar Town Ship SO"
    ]
  },
  "731133": {
    "pincode": "731133",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Churar BO",
      "Paigora BO",
      "Ranipathar BO",
      "Panchrahat SO"
    ]
  },
  "731201": {
    "pincode": "731201",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bagrakonda BO",
      "Bataspur BO",
      "Belia BO",
      "Bhalkuti BO",
      "Bhoromorekol BO",
      "Chotosangra BO",
      "Chowhatta BO",
      "Dewas Chandpur BO",
      "Hatia BO",
      "Ikra Hazrapur BO",
      "Kheyerpara BO",
      "Konarpur BO",
      "Kuchuighata BO",
      "Kurumsha BO",
      "Laghosa BO",
      "Mahodory BO",
      "Nimgoria BO",
      "Nirisha BO",
      "Paharpur BO",
      "Purbasiur BO",
      "Sindurtopa BO",
      "Uttarkhayerbuni BO",
      "Ahmadpur SO",
      "Iswarpurndtso SO"
    ]
  },
  "731202": {
    "pincode": "731202",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Barakartickchungri BO",
      "Dakhalbati BO",
      "Joykrishnapur BO",
      "Kaluha BO",
      "Laha BO",
      "Mallickpur BO",
      "Margram BO",
      "Ningha BO",
      "Baswa SO Birbhum"
    ]
  },
  "731204": {
    "pincode": "731204",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bolpur SO",
      "Adityapur BO",
      "Albandha BO",
      "Bagdowra BO",
      "Darpasila BO",
      "Kuchligopalpur BO",
      "Laldaha BO",
      "Muluk BO",
      "Raipur BO",
      "Rajatpur BO",
      "Sian BO",
      "Sitapur BO",
      "Supur BO",
      "Bolpur Bazar SO",
      "Bolpur Trisulapatty SO",
      "Bolpur Ukilpatty SO",
      "Bolpur Court BO"
    ]
  },
  "731213": {
    "pincode": "731213",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Brahmanbahara BO",
      "Chhamna BO",
      "Chhototurigram BO",
      "Daspalsa BO",
      "Kaleswar BO",
      "Kanutia BO",
      "Kotasur BO",
      "Kuliara BO",
      "Kusumi BO",
      "Mohurapur BO",
      "Parulia BO"
    ]
  },
  "731214": {
    "pincode": "731214",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Chunpalasi BO",
      "Dhalla BO",
      "Dumrut BO",
      "Ghurisha BO",
      "Hansra BO",
      "Koyra BO",
      "Latbhabanipur BO",
      "Matikona BO",
      "Moukhira BO",
      "Paikuni BO",
      "Paschimnarayanpur BO",
      "Payer BO",
      "Sunmuni BO",
      "Ushardihi BO",
      "Illambazar SO"
    ]
  },
  "731215": {
    "pincode": "731215",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bandar BO",
      "Belai BO",
      "Brahmankhanda BO",
      "Gannaserandi BO",
      "Palita BO",
      "Sakodda BO",
      "Saraswatibazar BO",
      "Srikrishnapur BO",
      "Thupsara BO",
      "Khujutipara SO"
    ]
  },
  "731216": {
    "pincode": "731216",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bharkata BO",
      "Dabuk BO",
      "Damra BO",
      "Dighalgram BO",
      "Dwaruri BO",
      "Gonpur BO",
      "Kanachi BO",
      "Kastogoria BO",
      "Katigram BO",
      "Makdamnagar BO",
      "Mohula BO",
      "Muruli Dangal BO",
      "Sekpur BO",
      "Sonj BO",
      "Taloan BO",
      "Tarachua BO",
      "Mallarpur SO"
    ]
  },
  "731218": {
    "pincode": "731218",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Mayureswar SO"
    ]
  },
  "731219": {
    "pincode": "731219",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Amdole BO",
      "Bahadurpur BO",
      "Beliapalsa BO",
      "Bhimpur BO",
      "Bipranandigram BO",
      "Dhananjoypur BO",
      "Dumurgram BO",
      "Edrakpur BO",
      "Kahinagar BO",
      "Kalahapur BO",
      "Kanakpur BO",
      "Kathia BO",
      "Malaypur BO",
      "Nayagram BO",
      "Ramchandrapur BO",
      "Ruprampur BO",
      "Murarai SO"
    ]
  },
  "731220": {
    "pincode": "731220",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Barla BO",
      "Debagram BO",
      "Diha BO",
      "Gosainpur BO",
      "Kaitha BO",
      "Kalitha BO",
      "Kogram BO",
      "Modhura BO",
      "Mustofadanga BO",
      "Paikpara BO",
      "Tejhati BO",
      "Nalhati SO"
    ]
  },
  "731221": {
    "pincode": "731221",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bangsabati BO",
      "Bonha BO",
      "Dantura BO",
      "Gaganpur BO",
      "Harowa BO",
      "Hilora BO",
      "Jajigram BO",
      "Kasimnagar BO",
      "Mitrapur BO",
      "Panchahar BO",
      "Raturi BO",
      "Tirogram BO",
      "Paikar SO"
    ]
  },
  "731222": {
    "pincode": "731222",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Ambhua BO",
      "Bahutali BO",
      "Bandhaipur BO",
      "Bhabanipur Bhatra BO",
      "Bonmohurapur BO",
      "Bonorampur BO",
      "Boruagopalpur BO",
      "Kanaighat BO",
      "Omarpur BO",
      "Rajgramgram BO",
      "Sidhori BO",
      "Srirampur BO",
      "Rajgaon SO"
    ]
  },
  "731223": {
    "pincode": "731223",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Barapahari BO",
      "Beliamritunjoypur BO",
      "Dadpur BO",
      "Kusumba BO",
      "Narayanpur BO",
      "Swadhinpur BO",
      "Chakaipur BO",
      "R K Sikshapith SO"
    ]
  },
  "731224": {
    "pincode": "731224",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Rampurhat HO",
      "Mahajanpatty SO",
      "Nischintapur SO",
      "Railpar SO Birbhum"
    ]
  },
  "731233": {
    "pincode": "731233",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Atla BO",
      "Barsal BO",
      "Chakpara BO",
      "Dekhuria BO",
      "Gug BO",
      "Karkaria BO",
      "Kharbona BO",
      "Kharun BO",
      "Majkhanda BO",
      "Poprasahapur BO",
      "Sandhigorabazar BO",
      "Sandhyajole BO",
      "Tarapur BO",
      "Chandipur Tarapith SO"
    ]
  },
  "731234": {
    "pincode": "731234",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bagdola BO",
      "Barasija BO",
      "Basudevpur BO",
      "Beliakuricha BO",
      "Chautara BO",
      "Deriapur BO",
      "Derpur BO",
      "Dohira BO",
      "Fulur BO",
      "Gadadharpur RS BO",
      "Gorola BO",
      "Gunutia BO",
      "Hatora BO",
      "Jiwe BO",
      "Kumarpur BO",
      "Kundala BO",
      "Kunuri BO",
      "Majiara BO",
      "Mathpalsa BO",
      "Nowaparamahadipa BO",
      "Pathai BO",
      "Ramnagar BO",
      "Rangaipur BO",
      "Sahora BO",
      "Satpalsa BO",
      "Saugram BO",
      "Uchpur BO",
      "Ulkunda BO",
      "Uttarbonogram BO",
      "Sainthia SO",
      "Rakhakalitala SO",
      "Sainthia Bazar SO"
    ]
  },
  "731235": {
    "pincode": "731235",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Paruldanga BO",
      "Santiniketan SO"
    ]
  },
  "731236": {
    "pincode": "731236",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Belatisultanpur BO",
      "Benuria BO",
      "Bergram BO",
      "Bheramari BO",
      "Bishnukhanda BO",
      "Daranda BO",
      "Digha BO",
      "Kasba BO",
      "Khanjanpur BO",
      "Monoharpur BO",
      "Nachansaha BO",
      "Ruppur BO",
      "Sattore BO",
      "BITM BO",
      "Sriniketan SO"
    ]
  },
  "731237": {
    "pincode": "731237",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bandhkhola BO",
      "Bara BO",
      "Bhadrapur BO",
      "Gopalchak BO",
      "Jesthabhabanipur BO",
      "Kantagoria BO",
      "Krishnapur BO",
      "Noapara BO",
      "Salisanda BO",
      "Sitalgram BO",
      "Ujirpur BO",
      "Lohapur SO"
    ]
  },
  "731238": {
    "pincode": "731238",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bhadista BO",
      "Bishore BO",
      "Chhatina BO",
      "Duria BO",
      "Goalmal BO",
      "Jogai BO",
      "Khanpur BO",
      "Kusmore BO",
      "Rudranagar BO",
      "Sultanpur BO",
      "Chatra SO Birbhum"
    ]
  },
  "731240": {
    "pincode": "731240",
    "circle": "West Bengal Circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Bahiri SO",
      "Banagram BO",
      "Bangachhatra BO",
      "Gheedaha BO",
      "Hatserandi BO",
      "Jalandi BO",
      "Nahina BO",
      "Pafuri BO",
      "Panchsowa BO",
      "Sansat BO",
      "Singhee BO"
    ]
  },
  "731241": {
    "pincode": "731241",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Dunigram BO",
      "Koyemba BO",
      "Nonadanga BO",
      "Chandpara SO"
    ]
  },
  "731242": {
    "pincode": "731242",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Anantapurkanaipur BO",
      "Bujung BO",
      "Gopegram BO",
      "Sardha BO",
      "Kurumgram SO"
    ]
  },
  "731243": {
    "pincode": "731243",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Ayas BO",
      "Banior BO",
      "Bautia BO",
      "Bhabanandapur BO",
      "Chilinpur BO",
      "Haridaspur BO",
      "Harioka BO",
      "Sonarkundu BO",
      "Nalhati Township SO"
    ]
  },
  "731244": {
    "pincode": "731244",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Budhigram BO",
      "Debiparulia BO",
      "Tentulia BO",
      "Bishnupur SO Birbhum"
    ]
  },
  "731245": {
    "pincode": "731245",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Begunia BO",
      "Birchandrapur BO",
      "Ghoshgram BO",
      "Khemedda BO",
      "Pakhuria BO",
      "Sanakpur BO",
      "Turigram BO",
      "Dakshingram SO"
    ]
  },
  "731301": {
    "pincode": "731301",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Barha BO",
      "Belhati BO",
      "Charkolgram BO",
      "Mohanpur BO",
      "Pakurhans BO",
      "Sauta BO",
      "Uchkaran BO",
      "Ukrandi BO",
      "ChNanoor SO"
    ]
  },
  "731302": {
    "pincode": "731302",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Aligram BO",
      "Brahmandihi BO",
      "Brahmanpara BO",
      "Daronda Ranipara BO",
      "Daskalgram BO",
      "Debagram Anaipur BO",
      "Dhrubabati BO",
      "Fewgram BO",
      "Koreya BO",
      "Nurpur BO",
      "Palsa BO",
      "Patnil BO",
      "Kirnahar SO"
    ]
  },
  "731303": {
    "pincode": "731303",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "Abadanga BO",
      "Bhalas BO",
      "Bipratikuri BO",
      "Dwarka BO",
      "Gopalpur BO",
      "Indus BO",
      "Ishakpur BO",
      "Kamadpur BO",
      "Kuniara BO",
      "Kurumba BO",
      "Maheshpur BO",
      "Purbakadipur BO",
      "Tatinapara BO",
      "Thiba BO",
      "Labpur SO",
      "Labpur Mastali SO"
    ]
  },
  "731304": {
    "pincode": "731304",
    "circle": "West bengal circle",
    "region": "Kolkata Region",
    "division": "Birbhum Division",
    "offices": [
      "BUNIA BO",
      "BHATRA BO",
      "PUSHULIA BO",
      "KAZIPARA BO",
      "LANGALHATA BO",
      "KURUNNAHAR SO"
    ]
  },
  "732101": {
    "pincode": "732101",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Malda HO",
      "Baluchar SO",
      "Bansbari SO",
      "District School Board SO Malda",
      "Malda Court SO",
      "Mirchak SO",
      "Netaji Subhas Road SO",
      "Rabindra Avenue SO",
      "Rathbari SO",
      "Sarbamangalapally SO"
    ]
  },
  "732102": {
    "pincode": "732102",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Jhaljhalia Railway Colony SO",
      "Bairgachi BO",
      "Dakshin Alinagar BO",
      "Haripur BO",
      "Kadamtali BO",
      "Katlamari BO",
      "Koilabad BO",
      "Kumarganj RS BO",
      "Kutubganj BO",
      "Kutubsahar BO",
      "Mahakalbona BO",
      "Maharajpur BO",
      "Maliha BO",
      "Pirganj BO",
      "Rajadighi BO",
      "Rajapur BO",
      "Raninagar BO",
      "Sambalpur Tal BO"
    ]
  },
  "732103": {
    "pincode": "732103",
    "circle": "West bengal circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Gour BO",
      "Kajigram Chandipur BO",
      "Mukdumpur SO",
      "Kamalabari BO",
      "Kanchantar BO",
      "Rajbati Ramnagar BO",
      "Fulbari BO"
    ]
  },
  "732121": {
    "pincode": "732121",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Aiho SO",
      "Gandhinagar BO",
      "Gouramari BO",
      "Mahadevpur BO",
      "Sirshi BO"
    ]
  },
  "732122": {
    "pincode": "732122",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Bulbulchandi SO",
      "Agra Harishchandrapur BO",
      "Gopalpurhat BO",
      "Habibpur BO",
      "Jagjibanpur BO",
      "Kanturka BO",
      "Kendpukur BO",
      "Khoribari BO",
      "Kotalpur BO",
      "Manikora BO",
      "Nakail BO",
      "Singabad BO"
    ]
  },
  "732123": {
    "pincode": "732123",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Chanchal SO",
      "Arbora BO",
      "Bhingole BO",
      "Birosthali BO",
      "Debiganj BO",
      "Hatinda BO",
      "Hazarat Jalalpur BO",
      "Ishadpur BO",
      "Ismailpur BO",
      "Jagannathpur Maltola BO",
      "Malatipur BO",
      "Malikan BO",
      "Mallikpara BO",
      "Paraninagar BO",
      "Rasulpur BO",
      "Sadarpur BO",
      "Serpur Mukdumpur BO",
      "Singhia BO"
    ]
  },
  "732124": {
    "pincode": "732124",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Gajol SO",
      "Ahora BO",
      "Akalpur BO",
      "Arjunpur BO",
      "Babupur BO",
      "Badnagra BO",
      "Bagsarai BO",
      "Deotala BO",
      "Dhaoel BO",
      "Hatimari BO",
      "Ichahar BO",
      "Katikandar BO",
      "Katna BO",
      "Khanta BO",
      "Lakshmipur BO",
      "Malipara BO",
      "Mayna BO",
      "Molladighi BO",
      "Mudapur BO",
      "Panchpara BO",
      "Parail BO",
      "Salaidanga BO",
      "Shivajinagar BO",
      "Taherpur BO"
    ]
  },
  "732125": {
    "pincode": "732125",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Harishchandrapur SO",
      "Baghua BO",
      "Bansdol Daulatpur BO",
      "Barduary BO",
      "Bhaluka Bazar BO",
      "Dahua BO",
      "Daulatnagar BO",
      "Dhangara BO",
      "Fatehpur BO",
      "Hardamnagar BO",
      "Jagannathpur BO",
      "Kariali BO",
      "Khopakati BO",
      "Mahanandatola BO",
      "Mahendrapur BO",
      "Malior BO",
      "Maniknagar BO",
      "Miahat BO",
      "Milangarh BO",
      "New Sadlichak BO",
      "Pipla BO",
      "Sambalpur BO",
      "Sultannagar BO",
      "Talbangrua BO",
      "Talgachhi BO",
      "Talgramhat BO",
      "Talsur BO",
      "Harishchandrapur Bazar BO"
    ]
  },
  "732126": {
    "pincode": "732126",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Kaligaon SO",
      "Bheba BO",
      "Chandrapara BO",
      "Galimpur BO",
      "Goalpara BO",
      "Kushmai BO",
      "Motiharpur BO",
      "Mulaibari BO",
      "Naikanda BO",
      "Noorganj BO",
      "Santoshpur BO",
      "Kharba BO"
    ]
  },
  "732127": {
    "pincode": "732127",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Khejuriaghat SO",
      "Chamagram BO",
      "Charbabupur BO",
      "Gurutola BO",
      "Palgachhi BO"
    ]
  },
  "732128": {
    "pincode": "732128",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Old Malda SO",
      "Adina Station BO",
      "Alal BO",
      "Balia Nawabganj BO",
      "Barkol BO",
      "Budhia BO",
      "Dhumadighi BO",
      "Harkharkha BO",
      "Jharpukhuria BO",
      "Jote Basanta BO",
      "Magurai BO",
      "Arapur BO"
    ]
  },
  "732138": {
    "pincode": "732138",
    "circle": "West Bengal Circle",
    "region": "North Bengal Region",
    "division": "Malda Division",
    "offices": [
      "Pakuahat SO",
      "Ahil BO",
      "Araji Jalsa BO",
      "Asrafpur BO",
      "Binodpur BO",
      "Dalla Madhyapara BO",
      "Dharmadanga BO",
      "Dighalbar BO",
      "Dohil BO",
      "Jagdala BO",
      "Kamaldanga BO",
      "Khutadaha BO",
      "Masheshpur BO",
      "Pannapur BO",
      "Parbatidanga BO",
      "Patul BO",
      "Purba Ranipur BO",
      "Rahutara BO",
      "Sapmari BO",
      "Bamongola BO"
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
