import threading

# buffer of opinions to process
opinion_buffer = []
opinion_buffer_lock = threading.Lock()

firebase_client = None

# As this is the only process that's allowed to modify firebase we can
# decrease read costs by an in memory cache
conversations_map = {}
_dirty_list_ids = set()

# Very simple scheme to avoid multiple writes to the same firestore element
def add_opinion(namespace, opinion):
    with opinion_buffer_lock:
        opinion_buffer.append((namespace, opinion))
        print (f"{namespace} : {opinion} buffered")
    process_buffer()

def process_buffer():
    global opinion_buffer
    with opinion_buffer_lock:
        print (f"Processing: {len(opinion_buffer)}")
        for (namespace, opinion) in opinion_buffer:
            print (f" processing {namespace} : {opinion}")
            assert namespace in NAMESPACE_REACTORS

            reactor = NAMESPACE_REACTORS[namespace]
            reactor(opinion)
        print (f"Processing complete")
        opinion_buffer = []
        _clean()

def _ensure_conversation_loaded(id):
    # If the conversation exists in firebase load it, otherwise create empty
    print (f"_ensure_conversation_loaded {id}")
    if id in conversations_map.keys():
        return

    doc = firebase_client.document(
        f'nook_conversation_shards/shard-0/conversations/{id}').get()

    if doc.exists:
        conversations_map[id] = doc.to_dict()
        return

    conversations_map[id] = _create_empty_conversation_map(id)

def _push_conversation(id):
    print ("_push_conversation {conv_id}")
    firebase_client.document(
        f'nook_conversation_shards/shard-0/conversations/{id}').set(conversations_map[id])

def _clean():
    global _dirty_list_ids
    for conv_id in _dirty_list_ids:
        _push_conversation(conv_id)
    _dirty_list_ids = set()


def _compute_message_id(opinion):
    return "id"

# {
#   'deidentified_phone_number': 'nook-phone-uuid-a55a8ddf-7bfc-49c3-a16d-ed0ff369a6b9',
#   'created_on': '2020-11-14T23:46:05.269955+00:00',
#   'text': 'T2',
#   'direction': 'in'
# }
def handle_sms_raw_msg(opinion):
    id = opinion["deidentified_phone_number"]
    _ensure_conversation_loaded(id)
    created_on = opinion["created_on"]
    text = opinion["text"]
    direction = opinion['direction']

    conversations_map[id]["messages"].append(
        {
            "datetime" : created_on,
            "direction" : direction,
            "text" : text,
            "translation" : "",
            "id" : _compute_message_id(opinion),
            "tags" : []
        }
    )
    _dirty_list_ids.add(id)


def handle_add_conversation_tags(opinion):
    id = opinion["deidentified_phone_number"]
    _ensure_conversation_loaded(id)
    for tag in opinion["tags"]:
        conversations_map[id]["tags"].add(tag)
    _dirty_list_ids.add(id)

def handle_remove_conversation_tags(opinion):
    id = opinion["deidentified_phone_number"]
    _ensure_conversation_loaded(id)
    for tag in opinion["tags"]:
        conversations_map[id]["tags"].remove(tag)
    _dirty_list_ids.add(id)

def handle_set_notes(opinion):
    id = opinion["deidentified_phone_number"]
    _ensure_conversation_loaded(id)
    conversations_map[id]["notes"] = opinion["notes"]
    _dirty_list_ids.add(id)

def handle_set_unread(opinion):
    print (f"WARNING: handle_set_unread not implemented")

    # id = opinion["deidentified_phone_number"]
    # _ensure_conversation_loaded(id)
    # conversations_map[id]["unread"] = True
    # _dirty_list_ids.add(id)

def handle_add_message_tags(opinion):
    # id = opinion["deidentified_phone_number"]
    # _ensure_conversation_loaded(id)
    print (f"WARNING: handle_add_message_tags not implemented")

def handle_remove_message_tags(opinion):
    id = opinion["deidentified_phone_number"]
    _ensure_conversation_loaded(id)
    print (f"WARNING: handle_remove_message_tags not implemented")

def handle_set_translation(opinion):
    id = opinion["deidentified_phone_number"]
    _ensure_conversation_loaded(id)
    print (f"WARNING: handle_set_translation not implemented")


def handle_set_suggested_replies(opinion):
    reply_map = {}

    # Mandatory fields
    reply_map['text'] = opinion["text"]
    reply_map['translation'] = opinion["translation"]
    # reply_map['__id'] = opinion["__id"]
    # __id = reply_map['__id']
    __id = opinion["__id"]

    # Defaulting keys
    reply_map["shortcut"] = opinion["shortcut"] if "shortcut" in opinion.keys() else ""

    # Optional keys
    if "seq_no" in opinion.keys():
        reply_map["seq_no"] = opinion["seq_no"]
    if "category" in opinion.keys():
        reply_map["category"] = opinion["category"]
    if "group_id" in opinion.keys():
        reply_map["group_id"] = opinion["group_id"]
    if "group_description" in opinion.keys():
        reply_map["group_description"] = opinion["group_description"]
    if "index_in_group" in opinion.keys():
        reply_map["index_in_group"] = opinion["index_in_group"]

    # Perform immediate write
    firebase_client.document(
        f'suggestedReplies/{__id}').set(reply_map)


def _create_empty_conversation_map(conversation_id):
    return {
        "deidentified_phone_number" : conversation_id,
        "demographicsInfo" : {},
        "messages" : [],
        "notes" : "",
        "tags" : [],
        "unread" : True
    }



NAMESPACE_REACTORS = {
    "nook_conversations/add_tags" : handle_add_conversation_tags,
    "nook_conversations/remove_tags" : handle_remove_conversation_tags,
    "nook_conversations/set_notes"  : handle_set_notes,
    "nook_conversations/set_unread"  : handle_set_unread,
    "nook_messages/add_tags" : handle_add_message_tags,
    "nook_messages/remove_tags" : handle_remove_message_tags,
    "nook_messages/set_translation" : handle_set_translation,
    "sms_raw_msg" : handle_sms_raw_msg,
    "nook/set_suggested_replies" : handle_set_suggested_replies
}
