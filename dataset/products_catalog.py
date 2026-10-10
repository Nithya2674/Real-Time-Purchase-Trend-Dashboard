"""
Instacart Market Basket Analysis - Product Catalog & Schema
Provides authentic Instacart products, departments, aisles, and pricing.
"""

DEPARTMENTS = {
    1: "frozen",
    2: "other",
    3: "bakery",
    4: "produce",
    5: "alcohol",
    6: "international",
    7: "beverages",
    8: "pets",
    9: "dry goods pasta",
    10: "bulk",
    11: "personal care",
    12: "meat seafood",
    13: "pantry",
    14: "breakfast",
    15: "canned goods",
    16: "dairy eggs",
    17: "household",
    18: "babies",
    19: "snacks",
    20: "deli",
    21: "missing"
}

# Authentic Instacart products with realistic base popularity weight and pricing
PRODUCTS_CATALOG = [
    # Top tier produce (classic Instacart leaders)
    {"product_id": 24852, "product_name": "Banana", "aisle": "fresh fruits", "department": "produce", "weight": 95, "unit_price": 0.69},
    {"product_id": 13176, "product_name": "Bag of Organic Bananas", "aisle": "fresh fruits", "department": "produce", "weight": 88, "unit_price": 1.99},
    {"product_id": 21137, "product_name": "Organic Strawberries", "aisle": "fresh fruits", "department": "produce", "weight": 78, "unit_price": 3.49},
    {"product_id": 21903, "product_name": "Organic Baby Spinach", "aisle": "packaged vegetables fruits", "department": "produce", "weight": 72, "unit_price": 2.99},
    {"product_id": 47209, "product_name": "Organic Hass Avocado", "aisle": "fresh fruits", "department": "produce", "weight": 68, "unit_price": 1.79},
    {"product_id": 47766, "product_name": "Organic Avocado", "aisle": "fresh fruits", "department": "produce", "weight": 58, "unit_price": 1.59},
    {"product_id": 47626, "product_name": "Large Lemon", "aisle": "fresh fruits", "department": "produce", "weight": 52, "unit_price": 0.79},
    {"product_id": 26209, "product_name": "Limes", "aisle": "fresh fruits", "department": "produce", "weight": 48, "unit_price": 0.59},
    {"product_id": 27966, "product_name": "Organic Raspberries", "aisle": "fresh fruits", "department": "produce", "weight": 45, "unit_price": 3.99},
    {"product_id": 22935, "product_name": "Organic Yellow Onion", "aisle": "fresh vegetables", "department": "produce", "weight": 42, "unit_price": 1.29},
    {"product_id": 24964, "product_name": "Organic Garlic", "aisle": "fresh vegetables", "department": "produce", "weight": 38, "unit_price": 0.99},
    {"product_id": 45007, "product_name": "Organic Zucchini", "aisle": "fresh vegetables", "department": "produce", "weight": 36, "unit_price": 1.49},
    {"product_id": 39275, "product_name": "Organic Blueberries", "aisle": "fresh fruits", "department": "produce", "weight": 35, "unit_price": 4.29},
    {"product_id": 49683, "product_name": "Cucumber Kirby", "aisle": "fresh vegetables", "department": "produce", "weight": 33, "unit_price": 0.89},
    {"product_id": 28204, "product_name": "Organic Fuji Apple", "aisle": "fresh fruits", "department": "produce", "weight": 32, "unit_price": 2.49},
    {"product_id": 5876, "product_name": "Organic Lemon", "aisle": "fresh fruits", "department": "produce", "weight": 30, "unit_price": 0.89},
    {"product_id": 8277, "product_name": "Apple Honeycrisp", "aisle": "fresh fruits", "department": "produce", "weight": 28, "unit_price": 2.99},
    {"product_id": 30391, "product_name": "Organic Gala Apples", "aisle": "fresh fruits", "department": "produce", "weight": 27, "unit_price": 2.29},
    {"product_id": 17794, "product_name": "Organic Whole Baby Carrots", "aisle": "packaged vegetables fruits", "department": "produce", "weight": 26, "unit_price": 1.69},
    {"product_id": 43352, "product_name": "Organic Blackberries", "aisle": "fresh fruits", "department": "produce", "weight": 25, "unit_price": 3.89},
    {"product_id": 4920, "product_name": "Seedless Red Grapes", "aisle": "fresh fruits", "department": "produce", "weight": 24, "unit_price": 2.99},
    {"product_id": 28842, "product_name": "Jalapeno Peppers", "aisle": "fresh vegetables", "department": "produce", "weight": 20, "unit_price": 0.49},

    # Dairy & Eggs
    {"product_id": 27845, "product_name": "Organic Whole Milk", "aisle": "milk", "department": "dairy eggs", "weight": 46, "unit_price": 4.59},
    {"product_id": 19057, "product_name": "Organic Large Grade A Brown Eggs", "aisle": "eggs", "department": "dairy eggs", "weight": 44, "unit_price": 3.99},
    {"product_id": 30489, "product_name": "Original Almondmilk", "aisle": "soy lactosefree", "department": "dairy eggs", "weight": 37, "unit_price": 3.29},
    {"product_id": 48679, "product_name": "Greek Nonfat Yogurt Plain", "aisle": "yogurt", "department": "dairy eggs", "weight": 34, "unit_price": 1.49},
    {"product_id": 16797, "product_name": "Salted Butter Quarters", "aisle": "butter", "department": "dairy eggs", "weight": 29, "unit_price": 4.19},
    {"product_id": 21616, "product_name": "Half & Half Creamer", "aisle": "cream", "department": "dairy eggs", "weight": 28, "unit_price": 2.89},
    {"product_id": 31717, "product_name": "Organic Lowfat 1% Milk", "aisle": "milk", "department": "dairy eggs", "weight": 26, "unit_price": 4.29},
    {"product_id": 4210, "product_name": "Whole Milk Plain Greek Yogurt", "aisle": "yogurt", "department": "dairy eggs", "weight": 23, "unit_price": 1.69},
    {"product_id": 24184, "product_name": "Shredded Mozzarella Cheese", "aisle": "packaged cheese", "department": "dairy eggs", "weight": 22, "unit_price": 3.49},
    {"product_id": 11520, "product_name": "Sharp Cheddar Cheese Bar", "aisle": "packaged cheese", "department": "dairy eggs", "weight": 20, "unit_price": 3.79},

    # Beverages
    {"product_id": 44632, "product_name": "Sparkling Water Grapefruit", "aisle": "water seltzer juice", "department": "beverages", "weight": 40, "unit_price": 4.99},
    {"product_id": 35221, "product_name": "Sparkling Natural Mineral Water", "aisle": "water seltzer juice", "department": "beverages", "weight": 35, "unit_price": 2.29},
    {"product_id": 21617, "product_name": "100% Raw Coconut Water", "aisle": "refrigerated", "department": "beverages", "weight": 31, "unit_price": 3.99},
    {"product_id": 4605, "product_name": "Pure Sparkling Water Lime", "aisle": "water seltzer juice", "department": "beverages", "weight": 28, "unit_price": 4.99},
    {"product_id": 9076, "product_name": "Organic Orange Juice No Pulp", "aisle": "refrigerated", "department": "beverages", "weight": 27, "unit_price": 4.49},
    {"product_id": 12341, "product_name": "Cold Brew Coffee Lightly Sweetened", "aisle": "coffee", "department": "beverages", "weight": 23, "unit_price": 5.49},
    {"product_id": 13988, "product_name": "Organic Lemonade", "aisle": "refrigerated", "department": "beverages", "weight": 19, "unit_price": 3.19},

    # Snacks
    {"product_id": 33731, "product_name": "Organic Tortilla Chips Sea Salt", "aisle": "chips pretzels", "department": "snacks", "weight": 29, "unit_price": 3.99},
    {"product_id": 40706, "product_name": "White Corn Tortilla Chips", "aisle": "chips pretzels", "department": "snacks", "weight": 26, "unit_price": 2.79},
    {"product_id": 34126, "product_name": "Dark Chocolate 70% Cacao", "aisle": "candy chocolate", "department": "snacks", "weight": 24, "unit_price": 3.29},
    {"product_id": 28413, "product_name": "Roasted Almonds Lightly Salted", "aisle": "nuts seeds dried fruit", "department": "snacks", "weight": 22, "unit_price": 6.99},
    {"product_id": 14233, "product_name": "Organic Pita Bites Crackers", "aisle": "crackers", "department": "snacks", "weight": 21, "unit_price": 2.99},

    # Bakery
    {"product_id": 41220, "product_name": "Organic Sourdough Bread", "aisle": "bread", "department": "bakery", "weight": 27, "unit_price": 4.99},
    {"product_id": 19660, "product_name": "100% Whole Wheat Bread", "aisle": "bread", "department": "bakery", "weight": 25, "unit_price": 3.49},
    {"product_id": 5077, "product_name": "Multigrain English Muffins", "aisle": "breakfast bakery", "department": "bakery", "weight": 19, "unit_price": 3.29},
    {"product_id": 24489, "product_name": "Organic Everything Bagels", "aisle": "breakfast bakery", "department": "bakery", "weight": 18, "unit_price": 4.19},

    # Meat & Seafood
    {"product_id": 28985, "product_name": "Boneless Skinless Chicken Breasts", "aisle": "poultry counter", "department": "meat seafood", "weight": 30, "unit_price": 8.99},
    {"product_id": 44359, "product_name": "Organic Ground Beef 85/15", "aisle": "meat counter", "department": "meat seafood", "weight": 25, "unit_price": 7.49},
    {"product_id": 18465, "product_name": "Wild Atlantic Salmon Fillet", "aisle": "seafood counter", "department": "meat seafood", "weight": 22, "unit_price": 11.99},
    {"product_id": 46979, "product_name": "Uncured Applewood Smoked Bacon", "aisle": "hot dogs bacon sausage", "department": "meat seafood", "weight": 20, "unit_price": 5.99},

    # Pantry & Dry Goods
    {"product_id": 13870, "product_name": "Organic Extra Virgin Olive Oil", "aisle": "oils vinegars", "department": "pantry", "weight": 24, "unit_price": 9.99},
    {"product_id": 39877, "product_name": "Organic Penne Rigate", "aisle": "dry pasta", "department": "dry goods pasta", "weight": 22, "unit_price": 1.99},
    {"product_id": 24010, "product_name": "Organic Marinara Pasta Sauce", "aisle": "pasta sauce", "department": "pantry", "weight": 21, "unit_price": 3.79},
    {"product_id": 33198, "product_name": "Organic Diced Tomatoes in Juice", "aisle": "canned meals beans", "department": "canned goods", "weight": 20, "unit_price": 1.59},
    {"product_id": 19508, "product_name": "Organic Black Beans 15 oz", "aisle": "canned meals beans", "department": "canned goods", "weight": 19, "unit_price": 1.29},

    # Frozen
    {"product_id": 35951, "product_name": "Organic Frozen Wild Blueberries", "aisle": "frozen produce", "department": "frozen", "weight": 20, "unit_price": 4.49},
    {"product_id": 10246, "product_name": "Thin Crust Margherita Frozen Pizza", "aisle": "frozen meals", "department": "frozen", "weight": 19, "unit_price": 6.99},
    {"product_id": 25890, "product_name": "Vanilla Bean Ice Cream Pint", "aisle": "ice cream ice", "department": "frozen", "weight": 18, "unit_price": 4.99},

    # Deli & Household
    {"product_id": 46667, "product_name": "Organic Traditional Hummus", "aisle": "fresh dips spreads", "department": "deli", "weight": 21, "unit_price": 3.99},
    {"product_id": 21105, "product_name": "Oven Roasted Turkey Breast Slices", "aisle": "lunch meat", "department": "deli", "weight": 19, "unit_price": 4.99},
    {"product_id": 34320, "product_name": "Recycled Paper Towels 6-Pack", "aisle": "paper goods", "department": "household", "weight": 17, "unit_price": 8.49},
    {"product_id": 18018, "product_name": "Free & Clear Liquid Dish Soap", "aisle": "dish detergents", "department": "household", "weight": 16, "unit_price": 3.49}
]

PRODUCT_LOOKUP = {p["product_id"]: p for p in PRODUCTS_CATALOG}
