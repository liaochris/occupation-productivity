// this file explores the pay-productivity dataset
// (matching ACS and BLS data based on industry codes)


// load the dataset
cd "~/GitHub/occupation-productivity"
import delimited "Datasets/Merged/ACS_wage_productivity.csv", clear


// inspect the variables
describe
summarize
// codebook, compact (too long to run)


// prelim questions/comments
// - pull the non-detailed versions of educ and empstat
// - pull demos (race, gender)
// - pull the occ1990 (or occ2000? occ2010?) variables
// - is occsoc consistent through time? i.e. does the coding scheme not change?
// - pull the underlying variables used to construct hrwage
// - incwage, *hrs*, *wks*, cpi99 (pull all these together)
// - wages seem to sometimes fall mw, max a little over 400? mw in 1999 = 5.15
// - what's the top censor for income in the ACS data?
// - (>250k?) (maybe this determines the upper bound?) ($125/hr?)
// - pull the ind1990 (ind2000? ind2010?) variables (same as occ)
// - keep the BLS codes, keep the INDNAICS codes
// - what are the empstatd codes?
// - observation count slight decreasing over time, why?
// - store occ and ind as numerics, not strings
// - what's dup_cnt?
// - if we have m:m matches (1 ind_bls maps to N ind_acs or vice versa)
// - then we need weighting variable, e.g. share of ind_bls in ind_acs and vv
// ind_bls		ind_acs		fuzzy
// 1			A			40%
// 1			B			60%
// perwt2 = perwt*fuzzy
// - fuzzy = 1/dup_cnt?
// - productivity heavily skewed, so probably in levels and not logs/growth
// - tricky, needs a lot of documentation to make sure it's right
// - keep track of units that it's measured in
// - let's keep all the underlying variables, as much as possible
// - might want to merge in growth of these productivity scores (since it's unitless)
// - growth = D.log(prod)
// - bucket age into 5yr buckets, education into <hs, hs, cc, ba, grad?)
// - merge in [B1] and [B2] from https://ddorn.net/data.htm (need [A5] bridge)
// - other variables from Acemogolu papers?
// - merge in other variables from this site? e.g. trade?

encode occsoc, generate(occ)
drop occsoc
encode industry_code, generate(ind)
drop industry_code

replace perwt = perwt/dup_cnt
keep if empstatd==10 // some kind of employed?
drop if hrwage==0 | productivity==0

rename hrwage wage
rename productivity prod

foreach x of varlist wage prod {
	generate log_`x' = log(`x')
	reghdfe log_`x' [aweight = perwt], absorb(age educd) residuals(log_`x'2)
	replace log_`x'2 = log_`x'2 + _b[_cons]
	generate `x'2 = exp(log_`x'2)
	}

// w = f(age) + g(educ) + c + e
// I want e + c = wage unexplained by age and educ

gcollapse wage* prod* (rawsum) perwt [aweight = perwt], by(year occ ind)

foreach x of varlist wage* prod* {
	generate log_`x' = log(`x')
	}

binscatter log_wage log_prod [aweight = perwt], nq(100) reportreg absorb(year)
binscatter log_wage2 log_prod2 [aweight = perwt], nq(100) reportreg absorb(occ)

