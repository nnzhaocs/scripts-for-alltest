May 28
------

Read through section 3

General Comments:

001. We should put the paper in the SoCC layout. That'll make
 it easier to estimate, how much space we have left (usually
 the SoCC submission template is pretty wasteful).

002. Section 2 is not written yet but just in general, we
 have to be very careful that there's no (or only very little)
 overlap with the current CLUSTER submission. It's very bad
 to have the same content submitted at two different venues.

Comments:

001. When building the paper, Figure 5 seems to come out wrong.
 It's different compared to the Figure that's shown in the
 committed version of 'paper.pdf'. Make sure we have the correct
 Figure in there.

002. Does the Docker registry also store metadata in a NoSQL
 database? If not, why don't they? Is there any overhead?

003. Figure 5 (assuming it's the one that is in the committed
 'paper.pd') is pretty dense. It may be good to split the figure
 into subfigures and described push and pull separately. A third
 subfigure describing slices may also be useful. Currently, there's
 too much going on in the figure and it's hard to follow the flow.

004. We need to add a bit more detail in 3.1 on slices. How are files
 distributed? Where is the slice metadata kept? What exactly are
 we hashing to generate the slice ID (file contents or metadata
 content)?
Update: I saw you added those details in 3.2.2, maybe add a forward
 reference in 3.1 then?

005. The part on the client modifications in 3.1 is an implementation
 detail and should be moved to the implementation section.

006. Overall, the flow in 3.1 needs to be improved. At the moment, it
 only describes what is going on but we need to motivate better, we we
 made those design choices. What are the challenges that come with
 the registry design and which design choice is solving which challenge?
 This comment also applies to 3.2 and 3.3. Try to follow more of a
 top-down approach, i.e. first say what is the component/design choice,
 what problem is it solving, why is it important and challenging, and
 how is it solving the problem.

007. Figure 6 is a bit confusing as the y-axis seems to be log-scale
 but the last gap is not (2TB -> 3TB compared to 2TB -> 20TB). This makes
 it look like the benefit is saturating. Do we need the 3TB point? Can
 we get a larger sample (20TB would be best but smaller, e.g. 10TB, might
 also be ok)?

008. In 3.2.2 we say that deduplication depends on the current registry
 load (RPS) but we also say it runs periodically. When exactly does it
 run? Is there a configurable frequency (e.g. every 1h) but it only runs,
 if the registry load is below the defined threshold? Or is it trying to
 run on every push (then it's not periodic). Need to make that clear.

009. The named paragraph "Parallel slice restoring" at the end of 3.2.2
 seems a bit lost. Either structure the entire section in named
 paragraphs (which is usually a good idea) or just add it as a normal
 paragraph and remove the bold title.

010. Section 3.2.3 doesn't really flow well. We need a more top-down
 description in that section. Start by explaining, what is the problem
 (deduplication overhead), then describe the performance breakdown, and
 then introduce the LRA cache as a solution. Also don't mix present
 and past tense when describing an experiment. Choose one and then
 use it consistently.

011. "The overhead of slice copying can be largely mitigated for
 a large-scale registry [...]" -> It's not a good strategy to mention that
 there's a problem but it's probably solved at scale but then still
 introduce a complex mechanism to solve it. Also I'm not sure if that's
 true as there can be very large images so you'll still get a long tail
 for copying times. Also, if you only do round-robin distribution compared
 to size-based distribution, your slice sizes are probably going to be
 skewed. I think we should either remove this point here and put it at
 the end or better, evaluate copying times for an increasing number of
 registry nodes in the evaluation (with and without the LRA).

012. How does LRPA look up slice restoring profiles by size? Do we index
 the size attribute of the profile also in the database?

013. Why do we need to pick another slice with an acceptable restoring
 performance? Can't that just be a threshold? I though we already have
 theta_{rsfc} defined?

014. You say that "[...] slices for the same layer have similar sizes
 [...] because of unique file distribution". Do we know that? What
 if file sizes in an image are skewed?

015. The last paragraph in 3.2.3 again seems to be a bit out of context.
 This should be moved to 3.2.2 as 3.2.3 only talks about the LRA.

016. How long does it usually take after a pull manifest request before
 the first pull slice request comes in? Is it long enough to preconstruct
 slices and load them in the cache? Are we evaluating this?

017. At the end of the "User access patterns" analysis part, we should
 put some conclusion, i.e. what did we find and how are we going to use
 these findings to improve Sift.

018. What exactly do the user, repository, and layer profiles in URLmap
store? Is it the layer repel count and repository repelling probability
and user repelling probability as stated at the end of the paragraph?
If so, this should be made more explicit, i.e. something like: "The
layer profile contains the layer repel count for the corresponding
repository and user...".

019. In general, when putting an algorithm in a paper, you need to
 explain it in detail, i.e. explain the steps it is performing and
 add references to the lines which perform each step. This holds
 for all 3 algorithms.

020. I was confused by the UBLP cache. Does it store slices and layers?
 How much space is allocated for each of those? How is it decided, whether
 a slice or a layer is cached?

021. In 3.3.2, it would be good to first set the stage, before diving right
 into the analysis of the access patterns, i.e. what is it we are trying to
 find by analyzing those patterns and how can it help the evictions strategy.

