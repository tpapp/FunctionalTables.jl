module FunctionalTables

using ArgCheck: @argcheck
using DocStringExtensions: SIGNATURES, TYPEDEF
using Parameters: @unpack

import Base: length, IteratorSize, IteratorEltype, eltype, iterate, keys, show, merge

include("utilities.jl")
include("columns.jl")
include("sorting.jl")
include("tables.jl")

end # module
