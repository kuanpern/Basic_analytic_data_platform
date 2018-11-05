import numpy as np

def accumulative_operate(rec, operator, keys):
	output = {}
	for key in keys:
		vals = rec[key].dropna().values
		output[key] = operator(vals)
	# end for
	return output
# end def

def distinct_count(rec, keys):
	operator = lambda S: len(set(S))
	return accumulative_operate(rec, operator, keys)
# end def

def accumulative_sum(rec, keys):
	operator = lambda S: sum(S)
	return accumulative_operate(rec, operator, keys)
# end def

def accumulative_max(rec, keys):
	def operator(S):
		if len(S) == 0:
			return np.nan
		else:
			return max(S)
		# end if
	# end def
	return accumulative_operate(rec, operator, keys)
# end def

def accumulative_min(rec, keys):
	def operator(S):
		if len(S) == 0:
			return np.nan
		else:
			return min(S)
		# end if
	# end def
	return accumulative_operate(rec, operator, keys)
# end def

def accumulative_inc(rec, timer_key, keys):
	output = {}
	for key in keys:
		t, y = zip(*rec[[timer_key, key]].dropna().values)
		m, c = np.polyfit(t, y)
		output[key] = m
	# end for
	return output
# end def
