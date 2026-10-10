"""
Transaction generator for Instacart Market Basket Analysis.
Generates streaming transaction events and historical trailing 60-minute windows.
"""

import random
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
try:
    from dataset.products_catalog import PRODUCTS_CATALOG, PRODUCT_LOOKUP
except ImportError:
    from products_catalog import PRODUCTS_CATALOG, PRODUCT_LOOKUP

class InstacartTransactionGenerator:
    def __init__(self, seed: Optional[int] = 42):
        if seed is not None:
            random.seed(seed)
        self.order_counter = 5000000
        self.products = PRODUCTS_CATALOG
        self.weights = [p["weight"] for p in self.products]
        self.promoted_product_id: Optional[int] = None
        self.promotion_multiplier: float = 1.0

    def set_promotion(self, product_id: Optional[int], multiplier: float = 3.5):
        """Simulate flash sales or promotional surges to showcase same-day shifts."""
        self.promoted_product_id = product_id
        self.promotion_multiplier = multiplier

    def _sample_product(self) -> Dict[str, Any]:
        """Weighted sample reflecting Instacart purchasing patterns + active promotion."""
        if self.promoted_product_id:
            weights = []
            for p in self.products:
                w = p["weight"]
                if p["product_id"] == self.promoted_product_id:
                    w *= self.promotion_multiplier
                weights.append(w)
            return random.choices(self.products, weights=weights, k=1)[0]
        else:
            return random.choices(self.products, weights=self.weights, k=1)[0]

    def generate_single_transaction(self, base_timestamp: Optional[datetime] = None) -> Dict[str, Any]:
        """Generate a single purchased item transaction."""
        self.order_counter += 1
        now = base_timestamp or datetime.now(timezone.utc)
        product = self._sample_product()
        
        # Quantity purchased in this line item (mostly 1-2, occasionally up to 4)
        quantity = random.choices([1, 2, 3, 4], weights=[70, 20, 7, 3], k=1)[0]
        reordered = 1 if random.random() < 0.65 else 0
        add_to_cart = random.randint(1, 15)
        user_id = random.randint(10001, 99999)

        unit_price = product["unit_price"]
        total_price = round(unit_price * quantity, 2)

        return {
            "order_id": self.order_counter,
            "user_id": user_id,
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "department": product["department"],
            "aisle": product["aisle"],
            "order_hour_of_day": now.hour,
            "add_to_cart_order": add_to_cart,
            "reordered": reordered,
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price,
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "epoch_seconds": now.timestamp()
        }

    def generate_trailing_window(self, minutes: int = 60, total_events: int = 1200) -> List[Dict[str, Any]]:
        """
        Generates realistic historical transactions spread evenly across the trailing N minutes.
        Ensures the dashboard has rich trailing 60-minute data upon initial startup.
        """
        now = datetime.now(timezone.utc)
        events = []
        for _ in range(total_events):
            offset_seconds = random.uniform(0, minutes * 60)
            tx_time = now - timedelta(seconds=offset_seconds)
            event = self.generate_single_transaction(base_timestamp=tx_time)
            events.append(event)
        
        # Sort chronologically
        events.sort(key=lambda x: x["epoch_seconds"])
        return events

if __name__ == "__main__":
    gen = InstacartTransactionGenerator()
    sample = gen.generate_single_transaction()
    print("Sample Instacart Transaction:")
    print(sample)
