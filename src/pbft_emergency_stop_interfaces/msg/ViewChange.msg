# Vreme kreiranja VIEW-CHANGE poruke
builtin_interfaces/Time stamp

# Replika koja šalje VIEW-CHANGE poruku
int32 sender_id

# Novi view u koji replika želi da pređe
uint32 new_view

# Da li replika poseduje lokalni PREPARED sertifikat
bool has_prepared_certificate

# View u kojem je PREPARED sertifikat formiran
# Vrednost se ignoriše kada je has_prepared_certificate=false
uint32 prepared_view

# Redni broj prepared zahteva
# Vrednost 0 se koristi kada sertifikat ne postoji
uint64 prepared_sequence_number

# Podaci zahteva iz PREPARED sertifikata
string request_id
string request_digest
bool emergency_stop

# Različite backup replike čije PREPARE poruke
# čine lokalni prepared sertifikat
int32[] prepare_senders