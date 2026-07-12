# Tipovi PBFT poruka
uint8 REQUEST=0
uint8 PRE_PREPARE=1
uint8 PREPARE=2
uint8 COMMIT=3

# Vreme slanja poruke
builtin_interfaces/Time stamp

# Tip poruke: REQUEST, PRE_PREPARE, PREPARE ili COMMIT
uint8 message_type

# Identifikator pošiljaoca
# Klijentski čvor može koristiti vrednost -1.
int32 sender_id

# Identifikator primaoca
# Vrednost -1 označava broadcast svim replikama.
int32 recipient_id

# Trenutni PBFT view
uint32 view

# Redni broj zahteva koji dodeljuje primary replika
uint64 sequence_number

# Jedinstveni identifikator klijentskog zahteva
string request_id

# Hash sadržaja zahteva
string request_digest

# Vrednost o kojoj se postiže konsenzus
bool emergency_stop
