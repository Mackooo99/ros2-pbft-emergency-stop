# Vreme objavljivanja potvrđene odluke
builtin_interfaces/Time stamp

# PBFT instanca kojoj odluka pripada
uint32 view
uint64 sequence_number
string request_id
string request_digest

# Potvrđena vrednost replikovanog stanja
bool emergency_stop

# True samo kada je formiran potreban kvorum
bool committed

# Broj i zahtevani prag saglasnih COMMITTED replika
uint32 confirmation_count
uint32 required_confirmations

# Replike čiji su saglasni statusi formirali odluku
int32[] confirming_replicas
