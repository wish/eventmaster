# eventmaster

There are lots of events that happen in production such as deploys, bounces, scale-up, etc. Right now these events happen, but we have no centralized place to track them -- which means we have no centralized place to search them. This project would include:
- Creating an event store
- API for sending/firing events
- Potential integrations into other services for searching etc. (grafana annotations)
- Working with other pieces of infra to expose meaningful events
