"""
Brand Classification: International vs Local Israeli
Based on market research and brand recognition
"""

# International brands available online globally (ships to Israel)
INTERNATIONAL_BEAUTY_BRANDS = {
    # Premium Beauty & Cosmetics
    'L\'Oreal', 'לוריאל', 'L\'OREAL', 'LOREAL',
    'Maybelline', 'מייבלין', 'MAYBELLINE',
    'NYX', 'NYX PROFESSIONAL MAKEUP',
    'Essence', 'ESSENCE', 'אסנס',
    'Revlon', 'רבלון', 'REVLON',
    'Estée Lauder', 'אסתי לאודר', 'ESTEE LAUDER',
    'Lancôme', 'לנקום', 'LANCOME',
    'Vichy', 'VICHY',
    'Neutrogena', 'NEUTROGENA',
    'Nivea', 'ניוואה', 'NIVEA', 'NIVO',
    'Bobbi Brown', 'BOBBI BROWN',
    'Inglot', 'INGLOT',
    'Makeup Revolution', 'MAKEUP REVOLUTION', 'REVOLUTION BEAUTY',
    'Kiehl\'s', 'KIEHL', 'KIEHLS',
    'MAC', 'MAC COSMETICS',
    'YSL', 'Yves Saint Laurent', 'YVES SAINT',
    'Clinique', 'CLINIQUE',
    'Origins', 'ORIGINS',
    'Clarins', 'CLARINS',
    'Benefit', 'BENEFIT',
    'Urban Decay', 'URBAN DECAY',
    'Too Faced', 'TOO FACED',
    'Anastasia Beverly Hills', 'ANASTASIA',
    'Mario Badescu', 'MARIO BADESCU',
    'Pupa', 'PUPA', 'PUPA VAMP',

    # Skincare & Dermocosmetics
    'La Roche-Posay', 'LA ROCHE-POSAY',
    'CeraVe', 'CERAVE',
    'The Ordinary', 'THE ORDINARY',
    'Paula\'s Choice', 'PAULAS CHOICE',
    'Dove', 'דאב', 'DOVE', 'DOO',
    'Palmer\'s', 'PALMER', 'PALMERS',
    'Eucerin', 'EUCERIN',
    'Bioderma', 'BIODERMA',

    # Haircare
    'Pantene', 'פנטן', 'PANTENE',
    'Garnier', 'GARNIER',
    'Schwarzkopf', 'שסטוביץ', 'SCHWARZKOPF',
    'Tresemmé', 'TRESEMME',
    'OGX', 'OGX',
    'Head & Shoulders', 'HEAD SHOULDERS',
    'Herbal Essences', 'HERBAL ESSENCES',

    # Oral Care
    'Colgate', 'קולגייט', 'COLGATE',
    'Oral-B', 'אורל בי', 'ORAL-B', 'ORAL B',
    'Sensodyne', 'סנסודיין', 'SENSODYNE',
    'Listerine', 'ליסטרין', 'LISTERINE',
    'Aquafresh', 'AQUAFRESH',

    # Household & Cleaning
    'SNO', 'סנו', 'SNO', 'SANO',
    'Palmolive', 'פלמוליב', 'PALMOLIVE',
    'Henkel', 'הנקל סוד', 'HENKEL',
    'Finish', 'פיניש', 'FINISH',
    'Unilever', 'יוניליוור', 'UNILEVER',
    'Reckitt Benckiser', 'רקיט בנקיזר', 'RECKITT',
    'Lenor', 'לנור', 'LENOR',

    # Personal Care
    'Durex', 'דורקס', 'DUREX',
    'Tampax', 'טמפקס', 'TAMPAX',
    'Always', 'אולוויז', 'ALWAYS',
    'Gillette', 'GILLETTE',
    'Braun', 'BRAUN',
    'Philips', 'PHILIPS',
    'Satisfyer', 'SATISFYER',

    # Health & Wellness
    'Solgar', 'סולגר', 'SOLGAR',
    'Nature\'s Bounty', 'NATURES BOUNTY',
    'NOW Foods', 'NOW FOODS',
    'Garden of Life', 'GARDEN OF LIFE',

    # Eyewear
    'Oakley', 'OAKLEY', 'OAKLEY OKYS',
    'Ray-Ban', 'RAY-BAN', 'RAYBAN',

    # Contact Lenses
    'Acuvue', 'ACUVUE',
    'Bausch & Lomb', 'BAUSCH LOMB',

    # Food/Beverage (International)
    'Nestle', 'נסטלה', 'NESTLE',
    'Milka', 'מילקה', 'MILKA',
    'Ferrero', 'FERRERO',
    'Mars', 'MARS',
    'Disney', 'DISNEY',

    # Baby Products
    'Philips Avent', 'אוונט', 'AVENT', 'PHILIPS AVENT',
    'Nuvita', 'NUVITA',
    'Chicco', 'CHICCO',

    # Luxury Perfumes
    'Dolce & Gabbana', 'DOLCE', 'D&G',
    'Giorgio Armani', 'ARMANI', 'ארמני',
    'Prada', 'PRADA', 'פראדה',
    'Versace', 'VERSACE',
    'Calvin Klein', 'CALVIN KLEIN',
    'Hugo Boss', 'HUGO BOSS',
}

# Israeli/Regional brands (difficult to get internationally)
LOCAL_ISRAELI_BRANDS = {
    'לייף', 'LIFE', 'LIFE WELLNESS', 'לייף וולנס',
    'אלפא', 'ALPHA',
    'דיפלומט', 'DIPLOMAT',
    'קרליין', 'CARELINE',
    'רייס', 'RICE',
    'נורוליס', 'NORALIS',
    'לילית', 'LILIT',
    'ד"ר פישר', 'DR FISHER', 'DR. FISHER', 'מדי פישר',
    'אלקליל', 'ALKALIL',
    'סולתם', 'SOLTAM',
    'מורז', 'MORAZ',
    'דנשר', 'DANSHAR',
    'איתן', 'EITAN',
    'מרשל', 'MARSHAL',
    'SPIRULIFE',
    'ALN',
    'HK',
    'TOYBOX',
    'INCENSE',
    'BOMB',
    'EIGHTEEN',
    'ICON',
    'FUN FACTORY',
    'K-CARE',
    'BIBS',

    # Israeli Food Brands
    'עלית', 'ELITE',
    'שטראוס', 'STRAUSS', 'שטראוס גרופ',
    'אסם', 'OSEM', 'אוסם',
    'ויסוצקי', 'WISSOTZKY',
    'חוגלה', 'HOGLA',
    'טעמן', 'TAAMAN',

    # Israeli Health/Beauty
    'סודות המזרח', 'SECRETS OF THE EAST',
    'Health & Beauty', 'HEALTH & BEAUTY',
    'ניאופארם', 'NEOPHARM',
    'פרומדיקו', 'PROMEDICO',
    'ס.מדיק', 'S.MEDIC',
    'פיירי', 'FAIRY',
    'אחת שתיים', 'ACHAT SHTAYIM',
    'פרמה גורי', 'PHARMA GURI',
    'דודי קרמר', 'DUDI KRAMER',
    'מדיטרנד', 'MEDITERAND',
    'סמואלוב', 'SMOULOV',
    'ליימן שליסל', 'LYMAN SCHLESEL',
    'פלוריש', 'FLOURISH',
    'לטאפה', 'LETAFA',
    'ארד', 'ARDO',
    'אביב', 'AVIV',

    # Israeli Brands (various)
    'כמיפל', 'KMIPEL', 'כמיפל שיווק',
    'קמיל בלו', 'KAMIL BLUE',
    'SANNY',
    'MILUCCA',
    'BYNETA',
    'NIVO',
    'TINC', 'טינק',
    'FIFI',
    'CHISA',
    'MINENE',
    'ERROCA', 'ERROCA ERS', 'ERROCA ACTIVE',
    'LA BEAUTE',
    'SOFTBERRY',
    'LEAVES',
    'PRO HAIR',
    'MAC AIR & BLOW',
    'MAD BEAUTY',
    'FRANCE BEAUTY',
    'FRE',
    'CATEGORY',
    'so.ko',
    'EASY TOUCH',
    'SOLINGEN ERBE',
    'M.D.S.',
    'ORO',
    'KYLIE BY',
    'C&B',
    'WIN',
    'MY',
    'POP',
    'ACQUA',
    'TODAY',
    'AFTER SHOWER',
    'SKIN',
    'EVERY',
    'DOO',  # Not Dove/Doo confusion
    'Even',
    'ליידי ספיד',
    'אלטמן', 'ALTMAN',
    'אלפרגאטאס', 'ALPARGATAS',
    'דיאדרמין', 'DIADREMIN',
    'פיטנס', 'FITNESS',
    'מאמ', 'MOM',
}

# Generic/unknown brands
GENERIC_BRANDS = {
    'General', 'כללי', 'לא ידוע', 'GOOD PHARM', 'Good Pharm'
}

def classify_brand(brand_name):
    """Classify a brand as international, local, or generic"""
    if not brand_name:
        return 'unknown'

    brand_upper = brand_name.upper().strip()

    # Check generic first
    for generic in GENERIC_BRANDS:
        if generic.upper() in brand_upper or brand_upper in generic.upper():
            return 'generic'

    # Check international
    for intl_brand in INTERNATIONAL_BEAUTY_BRANDS:
        if intl_brand.upper() in brand_upper or brand_upper in intl_brand.upper():
            return 'international'

    # Check local
    for local_brand in LOCAL_ISRAELI_BRANDS:
        if local_brand.upper() in brand_upper or brand_upper in local_brand.upper():
            return 'local'

    # Default to unknown
    return 'unknown'

if __name__ == "__main__":
    # Test classification
    test_brands = [
        'לוריאל', 'L\'Oreal', 'NYX', 'Essence',
        'לייף', 'אלפא', 'דיפלומט',
        'General', 'כללי',
        'Vichy', 'Neutrogena', 'Colgate'
    ]

    for brand in test_brands:
        print(f"{brand:20} -> {classify_brand(brand)}")
