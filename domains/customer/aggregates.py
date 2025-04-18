from domains.customer.entities import Customer
from domains.customer.value_objects import Address, Preferences

class CustomerAggregate:
    """Represents the Customer Aggregate, encapsulating all related entities & value objects."""
    
    def __init__(self, customer: Customer, address: Address, preferences: Preferences):
        self.customer = customer  # Aggregate Root
        self.address = address
        self.preferences = preferences

    def update_address(self, new_address: Address):
        """Updates the customer's address."""
        self.address = new_address

    def update_preferences(self, new_preferences: Preferences):
        """Updates customer preferences."""
        self.preferences = new_preferences

    def deactivate_account(self):
        """Marks customer as inactive."""
        self.customer.is_active = False