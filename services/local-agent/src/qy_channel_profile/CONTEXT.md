# Channel Profile

This context describes auditable channel classification and profile estimates derived from immutable crawler snapshots. It separates observable facts, human-verified labels, local estimates, compatibility output, and historical Agent comparisons.

## Language

### Evidence And Truth

**Snapshot Evidence**:
Public channel, content, and comment observations frozen at one `as_of` boundary together with their available field lineage.
_Avoid_: Ground truth, current database row

**Field Lineage**:
The extractor, source system, observation boundary, and policy status associated with one snapshot field.
_Avoid_: A record-level source string treated as proof for every field

**Gold Label**:
A label independently assigned under a frozen guideline, with blind annotation and adjudication or an approved factual source.
_Avoid_: Grok output, local rule label, model prediction

**Diagnostic Reference**:
A frozen historical Agent output used only to describe compatibility differences after a candidate is frozen.
_Avoid_: Gold label, pseudo-label, threshold target

**Public Proxy**:
A locally defined estimate from public observations that is not equivalent to a platform Analytics metric.
_Avoid_: Measured audience fact, active subscriber ratio

### Classification

**Topic**:
What a channel is repeatedly about, independent of presentation format or communicative purpose.
_Avoid_: Format, genre, creator identity

**Purpose Or Genre**:
Why or in what communicative genre content is presented, such as Instructional, Review, News, or Storytelling.
_Avoid_: Topic, format

**Format**:
The observable delivery form, such as Short-form, Long-form, Live, Podcast, or Screen Capture.
_Avoid_: Topic, purpose

**Content Source**:
The production provenance of published content, such as original creator work or professional publisher output.
_Avoid_: Account entity type, extractor source

**Account Entity Type**:
Whether the channel represents an individual, a multi-creator team, an organization, or an unknown entity.
_Avoid_: Creator gender, content source, `brand_team` as unknown

**Unknown Topic**:
An explicit abstention because the snapshot does not support a stable topic assignment.
_Avoid_: A forced miscellaneous child, a low-confidence guess

### Dataset Roles

**Annotation Pilot**:
A blind dataset used to test whether a draft taxonomy and guideline can be applied consistently; it is not a final test set.
_Avoid_: Gold test, training corpus

**Gold Test**:
An independently sampled, frozen dataset used once for final quality acceptance after taxonomy and model choices are frozen.
_Avoid_: Diagnostic set, annotation pilot, development set

**Grok Shadow**:
A final, read-only compatibility run against frozen historical Grok output after a local bundle is frozen.
_Avoid_: Model selection, release gate, distillation

### Compatibility

**Legacy Projection**:
A lossy adapter from the internal facet model to the existing v1 category and ten-tag contract.
_Avoid_: Taxonomy truth, v2 training label
