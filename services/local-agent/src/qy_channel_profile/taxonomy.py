from __future__ import annotations

from types import MappingProxyType


_TREE = {
    "Beauty Creators": ("Handsome Men", "Beautiful Women"),
    "Fashion": (
        "Makeup", "Skincare", "Other Fashion", "Fashion News",
        "Fashion & Accessories", "Clothing", "Footwear", "Bags & Luggage",
        "Accessories", "Underwear & Loungewear", "Tattoo", "Hair & Wigs",
    ),
    "Parenting": ("Parenting Life", "Children", "Maternity & Baby"),
    "Food": (
        "Mukbang", "Wilderness Cooking", "Food Presentation", "Food Reviews",
        "Cooking Tutorials", "Food Knowledge",
    ),
    "Home": (
        "Gardening & Flowers", "Furniture & Appliances", "DIY Crafts",
        "Life Hacks", "Interior Design", "Tools & Hardware",
        "Kitchen Appliances & Dining",
    ),
    "Casual Vlogs": (
        "Portrait Clips", "Screen Recordings", "Photography", "Lifestyle Vlogs",
    ),
    "Music": ("Traditional Instruments", "Western Instruments", "Singing", "Music Knowledge"),
    "Dance": ("Dance Styles", "Square Dance", "Hand Dance"),
    "General Humanities & Society": ("Arts & Culture",),
    "Travel": (
        "Tour Guide Content", "Travel Vlogs", "Travel Photography",
        "Travel Guides", "Hotels & Stays",
    ),
    "Pets & Animals": (
        "Other Animals", "Animal Welfare", "Dogs", "Cats", "Pet News",
        "Other Pets", "Pet Supplies",
    ),
    "Self Improvement": ("Emotions & Psychology", "Career Skills"),
    "Education": (
        "K-12 Education", "Primary & Secondary School", "School News",
        "Other Campus Content", "Campus Life", "Campus Activities",
        "Exams & Certifications",
    ),
    "Tech": (
        "Mobile Tech", "Consumer Electronics", "Computers & PCs",
        "Gadgets & Devices", "Tech News", "Digital Devices",
        "Photography Equipment",
    ),
    "Gaming": (
        "Strategy Games", "Adventure Games", "Action Games", "Role-Playing Games",
        "Casual Games", "Mobile Games", "Gaming Hardware & Accessories",
    ),
    "Health & Wellness": (
        "First Aid Supplies", "Personal Health & Care", "Supplements",
        "Oral Care", "Sexual Health & Wellness",
    ),
    "Sports & Outdoors": (
        "Ball Sports", "Fitness", "Swimming & Water Sports", "Winter Sports",
        "Cycling", "Fishing", "Outdoor Adventure",
    ),
    "Automotive": (
        "Cars & Vehicles", "Car Care & Styling", "Auto Parts & Accessories",
        "Car Electronics", "Auto Repair", "Motorcycles",
    ),
    "Software & Internet": (
        "Artificial Intelligence", "Mobile Apps", "Computer Software", "Web3",
    ),
    "Uncategorized": ("Uncategorized",),
}

CHANNEL_CATEGORY_TREE = MappingProxyType(_TREE)
TAXONOMY_VERSION = "qy-taxonomy-v1"


def valid_categories(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    level_1 = value.get("level_1")
    level_2 = value.get("level_2")
    if level_1 not in CHANNEL_CATEGORY_TREE or not isinstance(level_2, list):
        return False
    if not 1 <= len(level_2) <= 3 or len(level_2) != len(set(level_2)):
        return False
    return all(item in CHANNEL_CATEGORY_TREE[level_1] for item in level_2)

