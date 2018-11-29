module FunctionalTables

using ArgCheck: @argcheck
using DocStringExtensions: SIGNATURES, TYPEDEF
using Parameters: @unpack
using IterTools: @ifsomething, imap
import Tables

using Base: Callable
import Base:
    length, eltype, IteratorSize, IteratorEltype, iterate,
    keys, show, merge, map, filter

include("utilities.jl")
include("sorting.jl")
include("columns.jl")
include("functionaltables.jl")
include("tables-interface.jl")
include("sort.jl")
include("grouping.jl")

end # module
