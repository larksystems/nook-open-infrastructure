import uuid


# Return a new message identifier
def generate_new_message_uuid():
    return f"nook-message-{uuid.uuid4()}"
