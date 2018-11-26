module FunctionalTables

using ArgCheck: @argcheck
using DocStringExtensions: SIGNATURES, TYPEDEF
using Parameters: @unpack
using IterTools: @ifsomething, imap
import Tables

using Base: Callable
import Base: length, IteratorSize, IteratorEltype, eltype, iterate, keys, show, merge, map

include("utilities.jl")
include("columns.jl")
include("sorting.jl")
include("functionaltables.jl")
include("tables-interface.jl")
include("grouping.jl")
include("sort.jl")
include("aggregation.jl")

end # module
