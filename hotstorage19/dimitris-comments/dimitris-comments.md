### Section 3.1

- Where does the acronym "LRPA" come from, is it LRLA?
  - layer restoring latency aware deduplication (LRPA deduplication)
- "As shown in Figure 5, client *A* creates a new hello- world image hello-world:new from the official im- age which only contains a single layer *L1*"
  - It's not clear how that happens. Perhaps take it step-by-step, for example: "Client A pushes a new version of hello-world, containing layer L2. Pushing that image corresponds to performing a PUT of layer to registry and a PUT of manifest to database. [Talk about first PUT]. [Talk about second PUT]. At that point, a new version, hello-world:new, is available in the registry".
- Because Figure 5 contains many elements and interactions, "as shown in Figure 5" may not be particularly helpful. I think it should be used when it's easy to pinpoint the part of Figure 5 you're referring to.
- What does "new manifest *M1:0*" mean? Make clear to reader that M1 corresponds to hello-world.
- "a layer slice of L2 can be constructed "
  - What does constructing a layer slice mean?
- I think the Pull/Push section should be placed after all components are described, or should become a higher-level overview and state upfront what the outline is going to be. Alternatively, build the solution step by step (no caching first, then add caching). Currently, 3.1 is hard to follow, need to take a step back.
- How do you find the location of a slice?


