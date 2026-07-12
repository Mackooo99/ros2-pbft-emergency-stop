# Vreme objavljivanja statusa
builtin_interfaces/Time stamp

# Identifikator PBFT replike
int32 node_id

# Trenutni PBFT view
uint32 view

# Trenutno obrađivani redni broj
uint64 sequence_number

# Identifikator trenutnog zahteva
string request_id

# Digest trenutnog zahteva
string request_digest

# Trenutna faza:
# IDLE, PRE_PREPARED, PREPARED, COMMITTED ili REJECTED
string phase

# Broj validnih PREPARE pošiljalaca
uint32 prepare_count

# Broj validnih COMMIT pošiljalaca
uint32 commit_count

# Da li je replika lokalno ispunila prepared uslov
bool prepared

# Da li je replika lokalno formirala commit kvorum
bool committed

# Lokalno replikovano stanje
bool emergency_stop

# Da li je replika konfigurisana kao vizantijska
bool is_byzantine

# Dodatna informacija za monitoring i demonstraciju
string detail
