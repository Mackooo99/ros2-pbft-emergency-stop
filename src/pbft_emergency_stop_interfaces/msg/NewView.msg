# Vreme kreiranja NEW-VIEW poruke
builtin_interfaces/Time stamp

# Novi primary koji šalje NEW-VIEW poruku
int32 sender_id

# View koji se aktivira ovom porukom
uint32 new_view

# VIEW-CHANGE poruke koje čine kvorum od 2f + 1 replika
pbft_emergency_stop_interfaces/ViewChange[] view_change_messages

# Da li je izabran zahtev koji treba nastaviti u novom view-u
bool has_selected_request

# Da li zahtev potiče iz prethodnog PREPARED sertifikata
bool selected_from_prepared_certificate

# Najviši prepared view iz kojeg je zahtev izabran
# Ignoriše se kada je selected_from_prepared_certificate=false
uint32 selected_prepared_view

# Redni broj zahteva koji se nastavlja
uint64 selected_sequence_number

# Izabrani zahtev
string request_id
string request_digest
bool emergency_stop