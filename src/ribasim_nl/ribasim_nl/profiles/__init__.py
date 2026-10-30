MIN_PROFILE_AREA = 10.0
# Generated profiles that never exceed this area are dropped: such a basin barely stores water,
# so structures and DiscreteControl change its level faster than the solver and control can follow.
MIN_GENERATED_PROFILE_AREA = 100.0
assert MIN_GENERATED_PROFILE_AREA >= MIN_PROFILE_AREA
