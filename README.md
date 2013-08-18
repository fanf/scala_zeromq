Around ZeroMQ in Scala
======================

Paris Scala User Group #37
---------------------------

This project contains some little examples about how ZeroMQ can be 
use in Scala, either witht the direct binding, or thanks to Akka.

That project only demos pub/sub and req/rep socket type, but ZeroMQ
is far reacher than that, see: http://zguide.zeromq.org/page:all


### Testing

That project need an installation of lib ZeroMQ on the testing
machine, plus python bindings for running python programs. 

It was tested with libzmq v.2.1, but it should work with more recent
version (nothing fanzy is demoed here)

### Project organization

- src/main/sources/python contains two client programs for pub/sub and req/rep
  example. Hey, ZeroMQ is cool for trans-runtime communication!

- src/main/sources/psug/zeromq/pubsub demoes how to use PUB/SUB
  pattern, either directly or with Akka. 

- src/main/source/psug/zeromq/reqrep shows how req/rep can be used to
  build a "ask" pattern working out of the JVM, and use it to simulate
  a distributed admin/nodes command executor, where an admin send
  command to be execute into multiple nodes (à la Salt, Ansible)


### License 

All the code in that repository is under Apache Software Licence 2. 

Use it at your will ;)

### About the author

Hey, I'm @fanf42, aka François Armand, working at Normation, building 
best of bread (and easy) automation solution for your IT and cloud,
see http://rudder-project.org

Also, I'm coding in Scala since 2006 and like to participate to
the PSUG - for sur, the best French Scala User group. 

Cheers !

