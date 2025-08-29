.DEFAULT_GOAL := test

test:
	pylint tap_intercom --disable missing-docstring,logging-format-interpolation,no-member,arguments-differ,too-few-public-methods,line-too-long,too-many-arguments,too-many-locals,useless-object-inheritance,simplifiable-if-statement,protected-access,chained-comparison,inconsistent-return-statements,redefined-builtin,too-many-statements,too-many-nested-blocks,unused-variable,no-else-return,consider-using-f-string,unspecified-encoding,too-many-positional-arguments
