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

There's also a lot of logic involving how the interpolation of the remote state works on the client, and the math is really annoying.

The basic idea is:
- the client is always running ahead of the server's messages. 
  - Whatever the server state is, it's going to be delayed by a certain amount of latency
- However, it's fairly simple to predict what the incoming state of the server is going to be, by just running the simulator on the client machine
- The client does this by storing a certain amount of inputs in a ring buffer, and then whenever there's a new snapshots, run inputs on top of the snapshot to catch it up to where the client currently is.
  - This is why the client sends and receives acks on its inputs, it wants to knwo what's the latest input the server has received so it can remove it from the ring buffer.
- We also add a timestamp on each server snapshot coming in so that we can order incoming snapshots by time in a BTreeMap
  - This is so that we can prune old snapshots, and also correctly interpolate between snapshots to get an approximation of what the state on the server currently is actually like.
- We also send multiple inputs to the server bundled together just so the server state doesn't completely deviate from the client in case some inputs are lost
  - And even if some inputs ARE lost, the server will use dead reckoning to predict that the client was pressing the same button in between received inputs (since that's a easy guess to make)
  - We don't do any rewinding/rollback on the server side and then predicting like we do on the client side though. 
  - We COULD, but for now, it seems unnecessary.