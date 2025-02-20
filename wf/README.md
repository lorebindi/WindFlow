Questa versione di WindFlow è stata aggiornata in modo che per le applicazioni di StreamBenchmarks sia possibile specificare da terminale una stringa numerica per il thread pinning delle repliche degli operatori.

L'azione di pinning di un determinato thread è più conveniente effettuarla dal thread in questione. Pertanto, ogni replica di un qualsiasi operatore deve ricevere le informazioni necessarie per eseguire un corretto pinning del thread sottostante. Queste informazioni includono l'istanza di `pinning_thread_context` relativa all'operatore della replica, che contiene l'array di core da sfruttare, e l'indice della replica interno all'operatore. In questo modo, utilizzando l'indice della replica per accedere all'array di core, si ottiene il core corretto assegnato a quella replica.

Per realizzare tutto questo è stato necessario introdurre dei costruttori aggiuntivi per:

- Le classi builder degli operatori, come `Source_Builder`, `Map_Builder`, ...
- Le classi degli operatori, come `Source`, `Map`, ...
- Le classi delle repliche degli operatori, come `Source_Replica`, `Map_Replica`, ...

Questi costruttori consentono di fornire a ciascuna replica di ogni operatore l'istanza corretta della struct `pinning_thread_context` come attributo. In questo modo, una volta che WindFlow ha creato tutte le repliche di tutti gli operatori, ciascuna di esse avrà tutte le informazioni necessarie per effettuare il proprio pinning sul core corretto. Per eseguire questa operazione, viene sfruttato il metodo `svc\_init()`, ereditato dal livello FastFlow da ogni classe che rappresenta la replica di un operatore WindFlow. Questo metodo viene eseguito dal thread sottostante alla replica dell'operatore ad inizio elaborazione. Le operazioni aggiuntive in questo metodo sono:

- Pinnare il thread sul core corretto, individuabile a partire dall'indice della replica e dall'attributo di tipo `pinning_thread_context`, sfruttando la funzione `ff_mapThreadToCpu`. Questa funzione è un'astrazione fornita da FastFlow che si basa sulla system call `sched_setaffinity`.
- Sincronizzare la replica che rappresenta con le altre sulla barriera, in modo da garantire che l'elaborazione cominci quando tutti i thread sono stati pinnati correttamente sui corrispondenti core.

Una volta che tutte le repliche raggiungono la barriera l'elaborazione prosegue e l'applicazione parte con la sua esecuzione sfruttando solo i core specificati dall'utente da terminale.
