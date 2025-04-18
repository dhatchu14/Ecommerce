class Address:
    """Value Object representing a customer's address."""
    
    def __init__(self, street: str, city: str, state: str, zip_code: str):
        self.street = street
        self.city = city
        self.state = state
        self.zip_code = zip_code

class Preferences:
    """Value Object representing customer preferences."""
    
    def __init__(self, newsletter: bool, notifications: bool):
        self.newsletter = newsletter
        self.notifications = notifications