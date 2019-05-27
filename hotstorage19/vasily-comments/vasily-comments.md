May 26, 27
----------

Read through sections 3.0, 3.1, 3.2 only

Comments:

001. I first compiled the paper myself and Figure 5 became the wrong figure.
  As a result the text in Section 3.1 did not make sense to me.
  (In fact, I cannot even be sure whether the Latex text is the same 
  as the text in the committed PDF file!)
  To avoid such problems in the future, could we remove the
  PDF file and all auto-generated files from the paper repository?

002. The text in Section 3.1 is quite hard to understand at this point. Part of the problem
  is that the material we present is quite difficult. I strongly suggest to give
  the overview (Section 3.1) *without* involving the UBLP cache at all. 
  I think we should start from explaining the basic design: collection of registry servers,
  each server uses part of its storage for assembled layers and part for deduplicated files,
  which metadata is kept in the metadata store, etc. See more suggestions below
  for further simplification of the material material.

  I have also committed the diagram.pptx with some musings on how a simplified figure
  can look.

004. The explanation in Section 3.1 assumes that there are already some layers in
  the registry (or in the clients).  Could we start from even simpler scenario, when
  a client creats brand new image with only single layer?

006. It seems that Sift can store two image manifests: traditional (w/o slices) and "sliced".
  Clients seem to always upload traditional manifests only, but they are capable of interpreting
  sliced manifests to a) combine traditional manifests (to use on the client side) b) getting
  slices of the layers from registry servers c) assembling layers from the slices on
  the client side. I think we need to update the figure and the text to explain this:
  both what clients do and the fact that there are two manifest types.

007. Currently the text operates on the names of the images. But in reality, AFAIK,
  image names are mapped to image IDs. We need to explain this as well.
  
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

060. "Since there is no modification to the layer" -> "Since layers are immutable"

070. I Figure 5 I would use "L1ID" and "S1ID" to designate ids of the layers and slices.

080. "Removes the duplicated files from uncompressed L2" is unclear. It conveys that the
 L2 tarball (or directory) still exists but w/o duplicated files.

090. The paragraph on copy-on-write of image manifests is strange. Why do
  we need to do COW for manifest images? Why not just replace them?
