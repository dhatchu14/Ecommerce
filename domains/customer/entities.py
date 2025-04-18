class Customer:
    """Customer Entity representing a registered user."""
    
    def __init__(self, customer_id: int, name: str, email: str, is_active: bool = True):
        self.customer_id = customer_id
        self.name = name
        self.email = email
        self.is_active = is_active