# Rollback

The way the netcode in this project functions is through client/server rollback a la Quake 3 Arena.

These following links were very helpful in trying to figure this out:

- https://www.jfedor.org/quake3/ -> in depth explanation of the Quake 3 netcode
- https://2xko.riotgames.com/en-us/news/dev/how-2xko-handles-online-play/ - had a very pretty diagram to look at.
- https://www.gabrielgambetta.com/client-side-prediction-live-demo.html
  - This entire series is a godsend, but ESPECIALLY the live demo helped so much
  - To view the live demo, all you need to do is just "view source" on any browser
- https://www.snapnet.dev/blog/netcode-architectures-part-3-snapshot-interpolation/
  - This is also a really useful series in terms of understanding how snapshots work.

    
We currently don't do any rollback/prediction on the server side. 
As far as the server is concerned, it just wants the latest version of any inputs coming in.
How the client handles prediction is that it keeps a ring buffer of inputs coming in, and then whenever theres a new snapshot, it applies the ring buffer of local inputs on top of it.

There's also a lot of logic involving how the interpolation of the remote state works on the client, and the math is really annoying.

## TODO
I need to bundle multiple inputs into one packet so that the server can see what the other inputs might have been in case the server missed it.