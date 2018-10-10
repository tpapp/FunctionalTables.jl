module FunctionalTables

using ArgCheck: @argcheck
using DocStringExtensions: SIGNATURES, TYPEDEF
using Parameters: @unpack

import Base: length, IteratorSize, IteratorEltype, eltype, iterate, keys, show

include("utilities.jl")
include("columns.jl")
include("tables.jl")

end # module
