May 26
------

Read through sections 3.0, 3.1, 3.2 only

Comments:

001. I first compiled the paper myself and Figure 5 became the wrong figure.
  As a result the text in Section 3.1 did not make sense to me.
  (In fact, I cannot even be sure whether the Latex text is the same 
  as the text in the committed PDF file!)
  To avoid such problems in the future, could we remove the
  PDF file and all auto-generated files from the paper repository?

010. "Sift registry" is the collection of Sift registry servers and the metadata database.
  But the paper sometimes uses "Sift registry" term to refer to individual servers - both
  in Figure 5 and in the text. For clarity, I'd suggest to refer to individual
  servers as "Sift registry server" *everywhere* in the paper.

020.  Figure 5 and the text use exemplary clients A, B, and C to describe the pull and push 
  logic. The piece that is missing in 3.1 is how the clients decide to which registry server
  to connect?  Can they connect to any server?
  Or they identify the server based on the layer digest?
  How do they know the hostnames of the servers?
  Or is the assumption that each image manifest contains server hostnames?

030. I think Section 3.2.1 is in the wrong place. It does not belong to design, it is
  rather the motivation for deduplication. This motivation should go before the design
  section.

040. In many places the paper uses the term "push" for layers. Which is technically
  not correct.  The image is pushed, but the layer is put (or uploaded). Same applies
  to "layer pull" - such thing does not exist. But the client can get (or download) a layer.

050. 3.2.2: "Next, it computes a fingerprint for every file in the slices...". But the slices
  were not formed yet. The slices are formed few sentence later.

051. Somewhere, maybe in Section 3.1, we need to have a complete list of metadata (along with
  its goals) that the metadata database stores. Over time the text adds more and more
  pieces of metadata: file index, slice index, manifests, etc. and it would be nice
  to have all this pieces described in one place.
