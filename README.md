# SafeServer

This component is responsible for interacting with every party in d'Artagnan and securely process requests with SMPC protocols. 

# Dependencies

- ```smpc``` ([Secure Multi-Party Computation Library](https://github.com/d-artagnan-db/SMPC.git)
- ```testingutils``` ([Utility Testing component](https://github.com/d-artagnan-db/TestingUtils.git))
- ```hbaseInterfaces``` ([Database Interfaces](https://github.com/d-artagnan-db/HBaseInterfaces.git))
- ```protocommunications``` ([Protocol Buffers component](https://github.com/d-artagnan-db/ProtoCom.git))



# How to use Install SafeServer?

To Install SafeClient and any other dependency just do in each project.
mvn install -DskipTests


The project is an HBase coprocessor that can be installed according to HBases documentation.


