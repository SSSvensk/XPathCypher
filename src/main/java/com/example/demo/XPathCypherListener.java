package com.example.demo;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

//TODO
//AND

public class XPathCypherListener extends xpathBaseListener {

	private StringBuilder query = new StringBuilder();
	private Object appliesTo;
    private Object lastAttribute;
    private boolean lastNodeInPath;
    private boolean endPathStep;
	private boolean attribute;
	private String attributeName;
	private boolean attributeOnly = false;
    private boolean insidePredicate = false;
    private boolean insideStringFunction = false;
    private Object predicateValue;
    private boolean isNode = true;
    private boolean addPath = false;
    private int pathIndex = 0;
	private int aggregateIndex = 0;
	private boolean aggregateFilter = false;
    private String lastNode = "";
    private Output output = new Output();
    
    private boolean firstStep = true;
    
    private String functionName;
    private String functionValue;
    private ArrayList<String> axisNames = new ArrayList<String>();
    private Stack<String> functionNames = new Stack<String>();
    private Stack<Boolean> isNodeStack = new Stack<Boolean>();
    private Stack<Boolean> isLastNodeInPathStack = new Stack<Boolean>();
    
    private String previousAxis = "";
    private Stack<String> axisStack = new Stack<String>();
    
    private String axis = "";
    private String startEdge;
    private String endEdge;
    
    public StringBuilder priorityQuery = new StringBuilder();
    public StringBuilder predicateQuery = new StringBuilder();
    public Stack<StringBuilder> priorityQueries = new Stack<StringBuilder>();
    public Queue<StringBuilder> transitivePaths = new LinkedList<StringBuilder>();
    public StringBuilder unwindQuery = new StringBuilder();
	public StringBuilder withQuery = new StringBuilder();
	private StringBuilder optionalMatchQuery = new StringBuilder();
    private StringBuilder whereQuery = new StringBuilder();
    private String returnValue;
    private Stack<String> returnValues = new Stack<String>();
    public StringBuilder cypherQuery = new StringBuilder();
    
    
    private Stack<String> paths = new Stack<String>();
    private Stack<String> appliesToStack = new Stack<String>(); 
    
    private int aliasIndex = 0;

	public XPathCypherListener() {
        this.query = new StringBuilder();
    }

    public void setQuery(Object s) {
    	this.query.append(s);
    }

    public Object getQuery() {
    	return query;
    }

    @Override
    public void exitMain(xpathParser.MainContext ctx) {
    	System.out.println("Cypher query");
    	System.out.println(this.query);
    	System.out.println();
    	System.out.println("Translation done, now querying from database...");
    }
    
    @Override
    public void exitUnaryExprNoRoot(xpathParser.UnaryExprNoRootContext ctx) {
    	
    }
    
    @Override
    public void exitUnionExprNoRoot(xpathParser.UnionExprNoRootContext ctx) {
    	
    	
    }
    
    @Override
    public void exitPathExprNoRoot(xpathParser.PathExprNoRootContext ctx) {
    	
    }
    
    @Override
    public void exitLocationPath(xpathParser.LocationPathContext ctx) {
    	if (!this.insidePredicate) {
    		if (this.query.length() > 0) {
        		this.query.append(" UNION ");
        	}
        	this.query.append("MATCH ");
        	/*
        	while (this.transitivePaths.peek() != null) {
        		System.out.println("t path " + this.transitivePaths.peek().toString());
        		this.query.append(this.transitivePaths.poll().toString());
        	}*/
        	
        	this.query.append(this.priorityQuery);
        	this.query.append(this.predicateQuery);

			if (this.optionalMatchQuery.length() > 0) {
        		this.query.append(" OPTIONAL MATCH " + this.optionalMatchQuery);
        	}

			if (this.withQuery.length() > 0) {
        		this.query.append(" WITH " + this.withQuery);
        	}
        	
        	if (this.unwindQuery.length() > 0) {
        		this.query.append(" UNWIND " + this.unwindQuery);
        	}
        	if (this.whereQuery.length() > 0) {
        		this.query.append(" WHERE " + this.whereQuery);
        	}
        	
        	if (this.functionName != null && this.functionName != "not" && this.functionName != "count(*)" && this.functionName != "sum(*)" && this.functionName != "min(*)" && this.functionName != "max(*)") {
        		if (this.functionName.equals("substring")) {
        			this.query.append(" RETURN " + this.functionName + "(" + this.returnValue + ", " + this.predicateValue + ")");
        		} else {
        			this.query.append(" RETURN " + this.functionName + "(" + this.returnValue + ")");
        		}
        		
        	} else {
        		this.query.append(" RETURN " + this.returnValue);
        	}
        	this.returnValue = null;
        	this.isNode = true;
        	this.priorityQuery = new StringBuilder();
        	this.unwindQuery = new StringBuilder();
        	this.whereQuery = new StringBuilder();
    	}  	
    	//
    	
    }

    @Override
    public void enterAbsoluteLocationPathNoroot(xpathParser.AbsoluteLocationPathNorootContext ctx) {
    }
    
    @Override
    public void enterFunctionCall(xpathParser.FunctionCallContext ctx) {
    }
    
    @Override
    public void exitFunctionCall(xpathParser.FunctionCallContext ctx) {
    	if (this.insidePredicate) {
    		if (this.whereQuery.length() > 0) {
    			this.whereQuery.append(" AND ");
    		}
    		if (this.addPath) {
    			this.whereQuery.append("ALL(rel in relationships(" + this.paths.peek() + ") WHERE rel " + this.functionNames.pop() + " '" + this.functionValue + "')");
    		} else {
				if (this.functionName != "not" && this.functionName != "count(*)" && this.functionName != "max(*)" && this.functionName != "sum(*)" && this.functionName != "min(*)") {
					this.whereQuery.append(this.returnValue + " " + this.functionNames.pop() + " '" + this.functionValue + "'");
				}
    		}


			System.out.println(this.functionName);

			if (this.functionName == "count(*)") {
				if (this.withQuery.length() == 0) {
    			    this.withQuery.append(this.appliesTo);
    		    }
				this.withQuery.append(", count(" + this.returnValue + ") AS aggregate" + this.aggregateIndex);
				this.aggregateFilter = true;
				this.functionNames.pop();

			} else if (this.functionName == "max(*)") {
				if (this.withQuery.length() == 0) {
    			    this.withQuery.append(this.appliesTo);
    		    }
				this.withQuery.append(", max(" + this.returnValue + ") AS aggregate" + this.aggregateIndex);
				this.aggregateFilter = true;
				this.functionNames.pop();

			} else if (this.functionName == "min(*)") {
				if (this.withQuery.length() == 0) {
    			    this.withQuery.append(this.appliesTo);
    		    }
				this.withQuery.append(", min(" + this.returnValue + ") AS aggregate" + this.aggregateIndex);
				this.aggregateFilter = true;
				this.functionNames.pop();

			} else if (this.functionName == "sum(*)") {
				if (this.withQuery.length() == 0) {
    			    this.withQuery.append(this.appliesTo);
    		    }
				this.withQuery.append(", sum(" + this.returnValue + ") AS aggregate" + this.aggregateIndex);
				this.aggregateFilter = true;
				this.functionNames.pop();
			} else if (this.functionName == "not") {
				if (this.withQuery.length() == 0) {
    			    this.withQuery.append(this.appliesTo);
    		    }
				this.withQuery.append(", count(" + this.returnValue + ") AS aggregate" + this.aggregateIndex);

				if (this.whereQuery.length() > 0) {
    			    this.whereQuery.append(" AND ");
    		    }
				this.whereQuery.append("aggregate" + this.aggregateIndex + " = 0");
				this.aggregateFilter = true;
				this.functionNames.pop();
			}
    		
    		this.insideStringFunction = false;
    		
    		//Setting a new "dominant" function name from peek of name stack, if stack isn't empty
    		if (this.functionNames.isEmpty()) {
    			this.functionName = null;
    		} else {
    			this.functionName = this.functionNames.peek();
    		}
    		
    	} else {
    		if (this.functionName.equals("substring")) {
    			this.returnValue = this.functionName + "(" + this.returnValue + ", " + this.predicateValue + ")";
    			String a = this.query.toString().replaceFirst("null", this.predicateValue.toString());
    			this.query = new StringBuilder();
    			this.query.append(a);
    			
    		} else {
				if (this.functionName != "count(*)") {
					this.returnValue = this.functionName + "(" + this.returnValue + ")";
				}
    			
    		}
    		
        	this.functionNames.pop();
    	}
    	
    }
    
    @Override
    public void exitFunctionName(xpathParser.FunctionNameContext ctx) {
    	if (ctx.getChildCount() > 0) {
    		StringBuilder sb = new StringBuilder();
    		sb.append(ctx.getChild(0));
    		String cypherFunctionName = "";

			//possible function names inside predicate
    		if (this.insidePredicate) {
    			if (this.whereQuery.length() > 0) {
    		        this.whereQuery.append(" AND ");
     	        }
    			if (sb.toString().equals("contains")) {
        			cypherFunctionName = "CONTAINS";
					this.insideStringFunction = true;
        		} else if (sb.toString().equals("starts-with")) {
        			cypherFunctionName = "STARTS WITH";
					this.insideStringFunction = true;
        		} else if (sb.toString().equals("ends-with")) {
        			cypherFunctionName = "ENDS WITH";
					this.insideStringFunction = true;
        		} else if (sb.toString().equals("count")) {
        			cypherFunctionName = "count(*)";
        		} else if (sb.toString().equals("max")) {
        			cypherFunctionName = "max(*)";
        		} else if (sb.toString().equals("min")) {
        			cypherFunctionName = "min(*)";
        		} else if (sb.toString().equals("sum")) {
        			cypherFunctionName = "sum(*)";
        		} else if (sb.toString().equals("not")) {
					cypherFunctionName = "not";
        			//throw new IllegalArgumentException("Function NOT has not been implemented (yet). So try not to use it :)");
        		} else {
        			throw new IllegalArgumentException("String function name is invalid!");
        		}
    			
    		} else {
    			if (sb.toString().equals("avg")) {
    			    cypherFunctionName = "avg";
    		    } else if (sb.toString().equals("min")) {
    			    cypherFunctionName = "min";
    		    } else if (sb.toString().equals("max")) {
    		 	    cypherFunctionName = "max";
    		    } else if (sb.toString().equals("sum")) {
    			    cypherFunctionName = "sum";
    		    } else if (sb.toString().equals("count")) {
    			    cypherFunctionName = "count";
    		    } else if (sb.toString().equals("ceiling")) {
    			    cypherFunctionName = "ceil";
    		    } else if (sb.toString().equals("floor")) {
    			    cypherFunctionName = "floor";
    		    } else if (sb.toString().equals("round")) {
    			    cypherFunctionName = "round";
    		    } else if (sb.toString().equals("substring")) {
    		    	cypherFunctionName = "substring";
    		    } else if (sb.toString().equals("not")) {
    		    	throw new IllegalArgumentException("Function NOT has not been implemented (yet). So try not to use it :)");
    		    }  else {
    			    throw new IllegalArgumentException("Aggregate function name is invalid!");
    		    }
    		}
    		this.functionName = cypherFunctionName;
    		this.functionNames.push(cypherFunctionName);
    	}
    }

    @Override
    public void exitAbsoluteLocationPathNoroot(xpathParser.AbsoluteLocationPathNorootContext ctx) {
        if (this.insidePredicate) {
        	
        }
    }

    @Override
    public void exitNCName(xpathParser.NCNameContext ctx) {
    	StringBuilder sb = new StringBuilder();
    	String ncName = sb.append(ctx.getChild(0)).toString();
    	
    	if (this.firstStep && this.axis.equals("attribute")) {
    		if (this.addPath) {
    			this.returnValue = ncName;
    		} else if (this.insidePredicate) {
    			this.returnValue = this.appliesToStack.peek() + "." + ncName;
    		} else {
    			this.priorityQuery.append("(a"+ this.aliasIndex +")");
        		this.returnValue = "a" + this.aliasIndex + "." + ncName;
    		}
    		//this.aliasIndex++;
    		this.attributeOnly = true;
    		this.attribute = true;
    		this.attributeName = ncName;
    	} else if (this.axis.equals("attribute")) {
    		this.attribute = true;
    		this.attributeName = ncName;
    		if (this.appliesToStack.size() > 0) {
    			this.returnValue = this.appliesTo + "." + ncName;
    		} else {
    			this.returnValue = this.returnValue + "." + ncName;
    		}
    		
    	} else if (this.isNode) {
			if (this.insidePredicate && this.functionName == "not") {
				if (this.optionalMatchQuery.length() == 0) {
					this.optionalMatchQuery.append("(" + this.appliesTo + ")");
				}
				this.returnValue = "a" + this.aliasIndex;
				this.optionalMatchQuery.append("(" + this.returnValue + ":" + ncName + ")");
			} else {
				this.returnValue = "a" + this.aliasIndex;
				this.priorityQuery.append("(" + this.returnValue + ":" + ncName + ")");
    		    this.lastNode = this.returnValue;
			}

			this.aliasIndex++;
    		
    		if (!(Character.isUpperCase(ncName.charAt(0)) && ncName.substring(1,  ncName.length() - 1).equals(ncName.substring(1,  ncName.length() - 1).toLowerCase()))) {
    			String warnMessage = "In Neo4J nodes are usually named with capital letter followed by lower case letters.";
    			String details = ncName;
    			output.printWarning(warnMessage, details);
    		}
    	} else if (!this.isNode && !this.insideStringFunction) {

			if (this.insidePredicate && this.functionName == "not") {
				this.returnValue = "a" + this.aliasIndex;
				if (this.optionalMatchQuery.length() == 0) {
					this.optionalMatchQuery.append("(" + this.appliesTo + ")");
				}
				this.optionalMatchQuery.append("-[" + this.returnValue + ":" + ncName + "]->");
			} else {
				this.returnValue = "a" + this.aliasIndex;
    		

    		if (this.priorityQueries.size() > 0 && this.firstStep) {
    			if (this.addPath) {
    				this.priorityQuery.append("(" + this.appliesToStack.peek() + ")");
    			} else {
    				this.priorityQuery.append(", (" + this.appliesToStack.peek() + ")");
    			}
    			
    		}
    		
    		//If transitive axis, then ignore the alias name
    		if (this.axis.equals("ancestor") || this.axis.equals("descendant") || this.axis.equals("ancestor-or-self") || this.axis.equals("descendant-or-self")) {
    			this.priorityQuery.append(this.startEdge + ":" + ncName + "" + this.endEdge);
    			
    		//If parent or child, add alias name and the edge label.
    		} else if (this.axis.equals("parent") || this.axis.equals("child")) {
    			this.priorityQuery.append(this.startEdge + this.returnValue + ":" + ncName + "" + this.endEdge);
    			
    		//If no axis, this defaults to child
    		} else {
    			this.priorityQuery.append("-[" + this.returnValue + ":" + ncName + "]->");
    		}
    		
    		if (!ncName.equals(ncName.toUpperCase())) {
    			String warnMessage = "In Neo4J relations are usually named with uppercase letters.";
    			String details = ncName;
    			output.printWarning(warnMessage, details);
    		}
			}

			this.aliasIndex++;

    		
    		
    	} else if (this.insideStringFunction) {
    		if (this.firstStep && !this.returnValue.contains(".")) {
    			//throw new IllegalArgumentException("First argument of string function must be an attribute");
    		}
    		
    		this.functionValue = ncName;
    	}
    	if (this.insidePredicate) {
    		
    		if (!this.isNodeStack.peek() && !this.attribute) {
    			throw new IllegalArgumentException("Translator doesn't accept paths bound to edge.");
    		}
    		
    	}
    	this.firstStep = false;
    }
    
    @Override
    public void enterRelativeLocationPath(xpathParser.RelativeLocationPathContext ctx) {
    	this.firstStep = true;
    }

    @Override
    public void exitRelativeLocationPath(xpathParser.RelativeLocationPathContext ctx) {
    	
    	String pathVariable = "";
    	if (!this.paths.isEmpty()) {
    		pathVariable = this.paths.peek();
    	}
    	
    	if (this.isNode && this.attribute && !this.attributeOnly) {
    		this.priorityQuery.append("()");
    	} else if (!this.isNode && !this.attribute && !this.insideStringFunction) {
    		this.priorityQuery.append("()");
    		if (this.axis.equals("descendant") || this.axis.equals("descendant-or-self") || this.axis.equals("ancestor") || this.axis.equals("ancestor-or-self")) {
    			this.returnValue = "relationships(" + pathVariable + ")";
        	}
    	}
    	
    	boolean previousIsTransitive = false;
    	
    	if (this.previousAxis.equals("descendant") || this.previousAxis.equals("descendant-or-self") || this.previousAxis.equals("ancestor") || this.previousAxis.equals("ancestor-or-self")) {
    		previousIsTransitive = true;
    	} 
    	
    	if (this.isNode && this.attribute && previousIsTransitive) {
    		if (this.priorityQuery.charAt(0) != 'p') {
    			this.priorityQuery.insert(0, pathVariable + "=");
    		}
    		String newValue = "rel";
    		this.returnValue = newValue + "." + this.attributeName;
    		this.unwindQuery.append("relationships(" + pathVariable + ") AS " + newValue);
    	}
    	if (this.insidePredicate && this.priorityQuery.toString().length() > 0) {
    		//System.out.println("pq " + this.priorityQuery.toString());
    		/*
    		if (this.priorityQueries.size() > 1) {
    			this.priorityQuery = this.priorityQueries.pop().append(this.priorityQuery);
    		}
    		System.out.println("pushed " + this.priorityQuery);
    		
    		this.priorityQueries.push(this.priorityQuery);
    		System.out.println(this.priorityQueries);
    		this.priorityQuery = new StringBuilder();*/
    		
    	}
    	
    	this.isNode = false;
    }

    @Override
    public void enterStep(xpathParser.StepContext ctx) {
    	if (this.axis.length() > 0) {
    		this.previousAxis = this.axis;
    	}
    	
    	int indexOfCurrentChildNode = ctx.getParent().children.indexOf(ctx);
    	if (indexOfCurrentChildNode > 0) {
    		
    		//If preceding sibling was /
    		if (ctx.parent.getChild(indexOfCurrentChildNode - 1).toString().equals("/")) {
    			this.isNode = !this.isNode;
    		//If preceding sibling was //
    		} else if (ctx.parent.getChild(indexOfCurrentChildNode - 1).toString().equals("//")) {
    			this.priorityQuery.append("-[*]->");
    		} else {
    			throw new IllegalArgumentException();
    		}
    		
    		//
    		if (this.lastNodeInPath && !this.isNode) {
    			this.priorityQuery.append(", ");
    			this.priorityQuery = new StringBuilder();
    			this.priorityQuery.append("(" + this.returnValue + ")");
    		}
    		
    		
    	}
    }

    @Override
    public void exitStep(xpathParser.StepContext ctx) {
        
    }

    @Override
    public void exitPrimaryExpr(xpathParser.PrimaryExprContext ctx) {
    	//Checking if the child node is leaf node
    	if (ctx.getChild(0).getChild(0) == null) {
    		this.predicateValue = ctx.getChild(0);
    	}
    	
    }

    @Override
    public void exitAxisSpecifier(xpathParser.AxisSpecifierContext ctx) {
    	//Giving a new axis name
    	StringBuilder sb = new StringBuilder();
    	sb.append(ctx.getChild(0));

    	if (sb.toString().equals("parent")) {
    		this.axis = "parent";
    		this.startEdge = "<-[";
    		this.endEdge = "]-";
    	} else if (sb.toString().equals("ancestor")) {
    		this.startEdge = "<-[";
    		this.endEdge = "*]-";
    		this.axis = "ancestor";
    	} else if (sb.toString().equals("child")) {
    		this.startEdge = "-[";
    		this.endEdge = "]->";
    		this.axis = "child";
    	} else if (sb.toString().equals("descendant")) {
    		this.startEdge = "-[";
    		this.endEdge = "*]->";
    		this.axis = "descendant";
    	} else if (sb.toString().equals("ancestor-or-self")) {
    		this.startEdge = "<-[";
    		this.endEdge = "*0..]-";
    		this.axis = "ancestor-or-self";
    	} else if (sb.toString().equals("descendant-or-self")) {
    		this.startEdge = "-[";
    		this.endEdge = "*0..]->";
    		this.axis = "descendant-or-self";
    	} else if (sb.toString().equals("attribute") || sb.toString().equals("@")) {
    		this.axis = "attribute";
    	} else if (sb.toString() == null || !sb.toString().isEmpty()) {
    		this.startEdge = "-[";
    		this.endEdge = "]->";
    		this.axis = "child";
    	} else {
    		throw new IllegalArgumentException("Unknown axis " + sb.toString());
    	}
    	
    	if (sb.length() > 0) {
    		this.axisNames.add(this.axis);
    	}
    	
    }
    
    @Override
    public void enterEqualityExpr(xpathParser.EqualityExprContext ctx) {
    	if (this.insidePredicate) {
    		this.attribute = false;
    	}
    	
    }

    @Override
    public void exitEqualityExpr(xpathParser.EqualityExprContext ctx) {
    	StringBuilder sb = new StringBuilder();
    	sb.append(ctx.getChild(1));
        //Jos kyseessä on attribuutti, lisätään operaattori ja attribuutin arvo.
    	if (ctx.getChildCount() > 1) {
    		if (!this.attribute && this.aggregateFilter) {
				this.whereQuery.append("aggregate" + this.aggregateIndex + " " + ctx.getChild(1) + " " + this.predicateValue);
				this.aggregateFilter = false;
    			//throw new IllegalArgumentException("Left hand side of relational expression is not an attribute!");
    		} else {
				if (sb.toString().equals("=")) {
			    StringBuilder s = this.priorityQueries.pop();
			    if (s.charAt(s.length() - 1) == ')') {
				    if (s.charAt(s.length() - 2) == '}') {
					    s.insert(s.length() - 2, ", " + this.attributeName + ": " + this.predicateValue);
				    } else {
					    s.insert(s.length() - 1, " {" + this.attributeName + ": " + this.predicateValue + "}");
				    }
			    } else if (s.charAt(s.length() - 1) == '>') {
				    if (s.charAt(s.length() - 4) == '}') {
					    s.insert(s.length() - 4, ", " + this.attributeName + ": " + this.predicateValue);
				    } else {
					    s.insert(s.length() - 3, " {" + this.attributeName + ": " + this.predicateValue + "}");
				    }
			    } else if (s.charAt(s.length() - 1) == '-') {
				    if (s.charAt(s.length() - 3) == '}') {
					    s.insert(s.length() - 3, ", " + this.attributeName + ": " + this.predicateValue);
				    } else {
					    s.insert(s.length() - 2, " {" + this.attributeName + ": " + this.predicateValue + "}");
				    }  
			    }
			    this.priorityQueries.push(s);
		    } else if (sb.toString().equals("!=")) {
                if (this.attribute && this.insidePredicate && ctx.getChildCount() == 1) {
        	        if (this.whereQuery.length() > 0) {
        		        this.whereQuery.append(" AND ");
         	        }
                } else if (this.attribute && this.insidePredicate && ctx.getChildCount() > 1) {
        	        if (this.whereQuery.length() > 0) {
        		        this.whereQuery.append(" AND ");
        	        }
        	        if (this.addPath) {
        		        this.whereQuery.append("ALL(rel in relationships(" + this.paths.peek() + ") WHERE rel." + this.returnValue + " <> " + this.predicateValue + ")");	
        	        } else {
        		        this.whereQuery.append(this.returnValue + " <> " + this.predicateValue);
            	        this.attribute = false;
        	        }	
                }
		    }
			}
    	    
    	} else {
    		if (this.attribute && !this.insideStringFunction) {
    			if (this.whereQuery.length() > 0) {
    		        this.whereQuery.append(" AND ");
     	        }
    			if (this.addPath) {
    				this.whereQuery.append("ALL(rel in relationships(" + this.paths.peek() + ") WHERE EXISTS(rel." + this.returnValue + "))");
    			} else {
    				this.whereQuery.append("EXISTS(" + this.returnValue + ")");
    			}
    		
    		}
    	}
        /*
        if (this.insidePredicate) {
        	this.returnValue = this.returnValues.peek();
        }*/
        
    }

    @Override
    public void exitRelationalExpr(xpathParser.RelationalExprContext ctx) {
        //Jos kyseessä on attribuutti, lisätään operaattori ja attribuutin arvo.
    	/*
    	if (this.attribute && this.insidePredicate && ctx.getChildCount() > 1) {
        	if (this.whereQuery.length() > 0) {
        		this.whereQuery.append(" AND ");
        	}
        	this.whereQuery.append(this.returnValue + "" + ctx.getChild(1) + "" + this.predicateValue);
        	this.attribute = false;
        }*/
    	StringBuilder sb = new StringBuilder();
    	sb.append(ctx.getChild(1));
    	if (ctx.getChildCount() > 1) {
    		if (!this.attribute && this.aggregateFilter) {
				this.whereQuery.append("aggregate" + this.aggregateIndex + " " + ctx.getChild(1) + " " + this.predicateValue);
				this.aggregateFilter = false;
    			//throw new IllegalArgumentException("Left hand side of relational expression is not an attribute!");
    		} else {
				if (sb.toString().equals("=")) {
			    StringBuilder s = this.priorityQueries.pop();
			    if (s.charAt(s.length() - 1) == ')') {
				    if (s.charAt(s.length() - 2) == '}') {
					    s.insert(s.length() - 2, ", " + this.attributeName + ":" + this.predicateValue);
				    } else {
					    s.insert(s.length() - 1, " {" + this.attributeName + ":" + this.predicateValue + "}");
				    }
			    } else if (s.charAt(s.length() - 1) == '>') {
				    if (s.charAt(s.length() - 4) == '}') {
					    s.insert(s.length() - 4, ", " + this.attributeName + ":" + this.predicateValue);
				    } else {
					    s.insert(s.length() - 3, " {" + this.attributeName + ":" + this.predicateValue + "}");
				    }
			    } else if (s.charAt(s.length() - 1) == '-') {
				    if (s.charAt(s.length() - 3) == '}') {
					    s.insert(s.length() - 3, ", " + this.attributeName + ":" + this.predicateValue);
				    } else {
					    s.insert(s.length() - 2, " {" + this.attributeName + ":" + this.predicateValue + "}");
				    }  
			    }
			    this.priorityQueries.push(s);
		    } else {
                if (this.attribute && this.insidePredicate && ctx.getChildCount() > 1) {
        	        if (this.whereQuery.length() > 0) {
        		        this.whereQuery.append(" AND ");
        	        }
        	        if (this.addPath) {
        		        this.whereQuery.append("ALL(rel in relationships(" + this.paths.peek() + ") WHERE rel." + this.returnValue + " " + ctx.getChild(1) + " " + this.predicateValue + ")");	
        	        } else {
        		        this.whereQuery.append(this.returnValue + " " + ctx.getChild(1) + " " + this.predicateValue);
            	        this.attribute = false;
        	        }	
                }
		    }
    	    this.attribute = false;
			}
    	    
    	}
    }

    @Override
    public void enterAndExpr(xpathParser.AndExprContext ctx) {
    }


    @Override
    public void exitAndExpr(xpathParser.AndExprContext ctx) {
        if (ctx.getChildCount() > 1) {
        	//this.priorityQueries.pop();
        }
    }

    @Override
    public void enterOrExpr(xpathParser.OrExprContext ctx) {
        
    }


    @Override
    public void exitOrExpr(xpathParser.OrExprContext ctx) {
        if (ctx.getChildCount() > 1) {
        	output.printWarning("WARN: Logical operator OR hasn't been implemented. This query might behave unexpectedly.");
        }
    }


    @Override
    public void enterNodeTest(xpathParser.NodeTestContext ctx) {
    }

    @Override
    public void exitExpr(xpathParser.ExprContext ctx) {
        if (this.attribute && this.insidePredicate) {
            this.attribute = false;
        }
    }


    @Override
    public void exitNameTest(xpathParser.NameTestContext ctx) {
    	
        StringBuilder g = new StringBuilder();
        g.append(ctx.getChild(0));
        if (g.toString().equals("*")) {
			if (this.insidePredicate && this.functionName == "not") {
				if (this.optionalMatchQuery.length() == 0) {
					this.optionalMatchQuery.append("(" + this.appliesTo + ")");
				}
				if (this.isNode) {
        		    this.optionalMatchQuery.append("(a" + this.aliasIndex + ")");
        	    } else {
        		    this.optionalMatchQuery.append("-[a" + this.aliasIndex + "]->");
        	    }
			} else {
				if (this.isNode) {
        		    this.priorityQuery.append("(a" + this.aliasIndex + ")");
        	    } else {
        		    this.priorityQuery.append("-[a" + this.aliasIndex + "]->");
        	    }
			}
        	
        	this.returnValue = "a" + this.aliasIndex;
        	this.aliasIndex++;
        }
        
    }

    @Override
    public void enterPredicate(xpathParser.PredicateContext ctx) {
    	
    	//If predicate is bound to edge step, and the axis is transitive...
    	if (!this.isNode && (this.axis.equals("ancestor") || this.axis.equals("descendant") || this.axis.equals("ancestor-or-self") || this.axis.equals("descendant-or-self"))) {
			this.addPath = true;
			this.axisStack.push(this.axis);
			this.axis = "";
			
			String firstString = "";
			
			//If the query contains already edges attached to nodes, we have to split the query to fit
			//the path index to right place.
			//Only if the path isn't split before
			if (!this.priorityQuery.toString().contains(",") && (this.priorityQuery.toString().contains(")<") && this.priorityQuery.toString().contains("-(")) || (this.priorityQuery.toString().contains(">(")) || (this.priorityQuery.toString().contains(")-"))) {
			
				int a = this.priorityQuery.toString().lastIndexOf(")");
				firstString = this.priorityQuery.toString().substring(0, a + 1) + ", ";
				StringBuilder laterString = new StringBuilder();
				laterString.append("(" + this.lastNode + ")");
				laterString
						.append(this.priorityQuery.toString().substring(a + 1, this.priorityQuery.toString().length()));
				this.priorityQuery = laterString;
			}
			
			String pathId = "p" + this.pathIndex;
			
			if (this.insidePredicate) {
				this.priorityQuery.insert(2, pathId + "=");
			} else {
				this.priorityQuery.insert(0, pathId + "=");
			}
			
			this.priorityQuery.insert(0, firstString);
			
			this.paths.push(pathId);
			this.pathIndex++;
		} else {
			this.axisStack.push(this.axis);
			this.axis = "";
		}
    	this.isLastNodeInPathStack.push(this.lastNodeInPath);
    	this.lastNodeInPath = false;
    	this.appliesToStack.push(this.returnValue);
    	this.isNodeStack.push(this.isNode);
    	this.returnValues.push(this.returnValue);
    	this.priorityQueries.push(this.priorityQuery);
    	this.priorityQuery = new StringBuilder();
    	
    	//First step in predicate
    	this.firstStep = true;
    	this.isNode = false;
    	this.appliesTo = this.returnValue;
    	this.insidePredicate = true;
        	
    }

    @Override
    public void exitPredicate(xpathParser.PredicateContext ctx) {
    	this.lastNodeInPath = this.isLastNodeInPathStack.pop();
    	if (this.addPath) {
    		this.lastNodeInPath = true;
    	}
    	this.axis = this.axisStack.pop();
    	this.addPath = false;
    	
    	this.predicateQuery.insert(0, this.priorityQuery);
    	this.priorityQuery = this.priorityQueries.pop();

    	this.isNode = this.isNodeStack.pop();
    	this.returnValue = this.returnValues.pop();
    	this.appliesTo = this.appliesToStack.pop();
    	this.lastNode = this.appliesTo.toString();
    	this.attributeOnly = false;
    	if (!this.paths.isEmpty()) {
    		//this.paths.pop();
    	}
    	
    	if (this.appliesToStack.size() == 0) {
    		this.insidePredicate = false;
    		//this.axis = "";
    	}
    }

    @Override
    public void exitAbbreviatedStep(xpathParser.AbbreviatedStepContext ctx) {
    	StringBuilder g = new StringBuilder();
        g.append(ctx.getChild(0));
        if (g.toString().equals("..")) {
			if (this.insidePredicate && this.functionName == "not") {
				if (this.optionalMatchQuery.length() == 0) {
					this.optionalMatchQuery.append("(" + this.appliesTo + ")");
				}
				if (this.isNode) {
        		    this.optionalMatchQuery.append("(a" + this.aliasIndex + ")");
        	    } else {
        		    this.optionalMatchQuery.append("<-[a" + this.aliasIndex + "]-");
        	    }
			} else {
				if (this.isNode) {
        		    this.priorityQuery.append("(a" + this.aliasIndex + ")");
        	    } else {
        		    this.priorityQuery.append("<-[a" + this.aliasIndex + "]-");
        	    }
			}

			this.returnValue = "a" + this.aliasIndex;
        	this.aliasIndex++;
        	
        }
    }
}