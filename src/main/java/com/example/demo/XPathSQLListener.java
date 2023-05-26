package com.example.demo;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.Collections;

//TODO
//AND

public class XPathSQLListener extends xpathBaseListener {

	private StringBuilder query = new StringBuilder();
	private String appliesTo;
	private boolean attribute;
	private String attributeName;
	private boolean attributeOnly = false;
    private boolean insidePredicate = false;
    private boolean insidePredicateFunction = false;
    private Object predicateValue;

    private Output output = new Output();

    private String firstTable = "";
    private String lastTable = "";
    
    private boolean firstStep = true;
    
    private String functionName;
    private String functionValue;
    private ArrayList<String> innerJoins = new ArrayList<String>();
    private ArrayList<String> tables = new ArrayList<String>();
    private Stack<String> functionNames = new Stack<String>();
    
    private String previousAxis = "";
    private Stack<String> axisStack = new Stack<String>();
    
    private String axis = "";
    
    public StringBuilder priorityQuery = new StringBuilder();
    public StringBuilder predicateQuery = new StringBuilder();
    public Stack<StringBuilder> priorityQueries = new Stack<StringBuilder>();
    public Queue<StringBuilder> transitivePaths = new LinkedList<StringBuilder>();
    private ArrayList<String> whereQuery = new  ArrayList<String>();
    private String returnValue;
    private Stack<String> returnValues = new Stack<String>();
    public StringBuilder cypherQuery = new StringBuilder();
    
    
    private Stack<String> paths = new Stack<String>();
    private Stack<String> appliesToStack = new Stack<String>(); 

	public XPathSQLListener() {
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

            this.query.append("SELECT ");

            //define SELECT clause content.
            String selected = "";
			if (this.tables.size() > 1 && (this.functionName == "json" || this.functionName == "csv" || this.functionName == "xml")) {
				for (int i = 0; i < this.tables.size(); i++) {
					selected = selected + "'' AS " + this.tables.get(i) + "_dummy, " + this.tables.get(i) + ".*";
					if (i < this.tables.size() - 1) {
						selected = selected + ", ";
					}
				}
				//selected = String.join(".*, ", this.tables) + ".*";
			} else if (this.tables.size() > 1) {
                selected = this.lastTable + ".";
            } else {
                selected = "*";
            }

            if (this.functionName != null && this.functionName != "json" && this.functionName != "csv" && this.functionName != "xml") {
        		this.query.append(this.functionName + "(" + selected + "*) ");
        		
        	} else {
                if (this.returnValue == null || this.returnValue.equals("")) {
                    if (this.tables.size() > 1 && !(this.functionName == "json" || this.functionName == "csv" || this.functionName == "xml")) {
                        this.query.append(selected + "*");
                    } else {
                        this.query.append(selected);
                    }
                    
                } else {
                    if (this.tables.size() > 1) {
                        this.query.append(selected + "" + this.returnValue);
                    } else {
                        this.query.append(this.returnValue);
                    }
                    
                }
        	}

			if (this.functionName != "json" && this.functionName != "csv" && this.functionName != "xml") {
				Collections.reverse(this.innerJoins);
			}

			if (this.functionName != "json" && this.functionName != "csv" && this.functionName != "xml") {
				this.query.append(" FROM " + this.lastTable);
			} else {
				this.query.append(" FROM " + this.firstTable);
			}
        	
            
           
		    if (this.functionName != "json" && this.functionName != "csv" && this.functionName != "xml") {
				Collections.reverse(this.innerJoins);
			}

            this.query.append(String.join(" ", this.innerJoins));

            if (this.whereQuery.size() > 0) {
				this.query.append(" WHERE ");
                this.query.append(String.join(" AND ", this.whereQuery));
			}
            
			this.query.append(";");
        	
        	this.returnValue = null;
        	this.priorityQuery = new StringBuilder();
        	//this.whereQuery = new StringBuilder();
    	} 
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

    		this.insidePredicateFunction = false;
    		
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
    			this.returnValue = this.functionName + "(" + this.returnValue + ")";
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

    		if (this.insidePredicate) {
    			if (sb.toString().equals("contains")) {
        			cypherFunctionName = "CONTAINS";
        		} else if (sb.toString().equals("starts-with")) {
        			cypherFunctionName = "STARTS WITH";
        		} else if (sb.toString().equals("ends-with")) {
        			cypherFunctionName = "ENDS WITH";
        		} else if (sb.toString().equals("not")) {
        			throw new IllegalArgumentException("Function NOT has not been implemented (yet). So try not to use it :)");
        		}else {
        			throw new IllegalArgumentException("String function name is invalid!");
        		}
    			this.insidePredicateFunction = true;
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
    		    } else if (sb.toString().equals("json")) {
    		    	cypherFunctionName = "json";
    		    } else if (sb.toString().equals("xml")) {
    		    	cypherFunctionName = "xml";
    		    } else if (sb.toString().equals("csv")) {
    		    	cypherFunctionName = "csv";
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
    	
    	if (this.axis.equals("attribute")) {
    		this.attribute = true;
    		this.attributeName = ncName;
    		this.returnValue = ncName;
    		
    	} else {
            if (this.tables.size() > 0) {
				System.out.println("inner join");
				final String joinType = this.functionName == "json" || this.functionName == "xml" || this.functionName == "csv" ? "LEFT JOIN" : "INNER JOIN";
                
				final String prevTable = this.tables.get(this.tables.size() - 1);

				if (this.functionName == "json" || this.functionName == "xml" || this.functionName == "csv") {
					final String innerJoinBegin = " " + joinType + " " + ncName + " ON " + prevTable;
                    if (this.axis == "parent") {
                        this.innerJoins.add(innerJoinBegin + ".id = " + ncName + "." + prevTable);
                    } else {
                        this.innerJoins.add(innerJoinBegin + "." + ncName + " = " + ncName + ".id");
                    }
				} else {
					final String innerJoinBegin = " " + joinType + " " + prevTable + " ON " + prevTable;
                    if (this.axis == "parent") {
                        this.innerJoins.add(innerJoinBegin + ".id = " + ncName + "." + prevTable);
                    } else {
                        this.innerJoins.add(innerJoinBegin + "." + ncName + " = " + ncName + ".id");
                    }
				}
            }
            this.tables.add(ncName);

			if (this.firstTable == "") {
				this.firstTable = ncName;
			}
			
            this.lastTable = ncName;
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
    	
    	boolean previousIsTransitive = false;
    	
    	if (this.previousAxis.equals("descendant") || this.previousAxis.equals("descendant-or-self") || this.previousAxis.equals("ancestor") || this.previousAxis.equals("ancestor-or-self")) {
    		previousIsTransitive = true;
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
    			
    		//If preceding sibling was //
    		} else if (ctx.parent.getChild(indexOfCurrentChildNode - 1).toString().equals("//")) {
    			this.priorityQuery.append("-[*]->");
    		} else {
    			throw new IllegalArgumentException();
    		}
    	}
    }

    @Override
    public void exitStep(xpathParser.StepContext ctx) {
        
    }

    @Override
    public void exitPrimaryExpr(xpathParser.PrimaryExprContext ctx) {
    	//Checking if the child node is leaf node
        StringBuilder sb = new StringBuilder();
    	sb.append(ctx.getChild(0));

    	if (ctx.getChild(0).getChild(0) == null) {
            
    		this.predicateValue = ctx.getChild(0);
    	}
    	
    }

    @Override
    public void exitAxisSpecifier(xpathParser.AxisSpecifierContext ctx) {
    	//Giving a new axis name
    	StringBuilder sb = new StringBuilder();
    	sb.append(ctx.getChild(0));

        final String axisName = sb.toString();

    	if (axisName.equals("parent") || axisName.equals("n")) {
    		this.axis = "parent";
    	} else if (axisName.equals("descendant-or-self") || axisName.equals("ancestor-or-self") || axisName.equals("ancestor") || axisName.equals("child") || axisName.equals("descendant")) {
    		this.axis = axisName;
    	} else if (sb.toString().equals("attribute") || sb.toString().equals("@")) {
    		this.axis = "attribute";
    	} else if (sb.toString() == null || !sb.toString().isEmpty()) {
    		this.axis = "child";
    	} else {
    		throw new IllegalArgumentException("Unknown axis " + sb.toString());
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

        this.whereQuery.add(this.appliesTo + "." + this.attributeName + "" + sb.toString() + "" + this.predicateValue);
        
        /*
        //Jos kyseessä on attribuutti, lisätään operaattori ja attribuutin arvo.
    	if (ctx.getChildCount() > 1) {
    		if (!this.attribute) {
    			throw new IllegalArgumentException("Left hand side of equality expression is not an attribute!");
    		}
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
                }
		    }
    	} else {
    		if (this.attribute && !this.insidePredicateFunction) {
    			if (this.whereQuery.length() > 0) {
    		        this.whereQuery.append(" AND ");
     	        }
    		
    		}
    	}*/

        /*
        if (this.insidePredicate) {
        	this.returnValue = this.returnValues.peek();
        }*/
        
    }

    @Override
    public void exitRelationalExpr(xpathParser.RelationalExprContext ctx) {
    	StringBuilder sb = new StringBuilder();
    	sb.append(ctx.getChild(1));
    	if (ctx.getChildCount() > 1) {
    		if (!this.attribute) {
    			throw new IllegalArgumentException("Left hand side of relational expression is not an attribute!");
    		}
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
		    }
    	    this.attribute = false;
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
    public void exitNodeTest(xpathParser.NodeTestContext ctx) {
        StringBuilder g = new StringBuilder();
        g.append(ctx.getChild(0));
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
        	
        }
        
    }

    @Override
    public void enterPredicate(xpathParser.PredicateContext ctx) {

        System.out.println("Enter predicate");
    	
    	//If predicate is bound to edge step, and the axis is transitive...
    	if ((this.axis.equals("ancestor") || this.axis.equals("descendant") || this.axis.equals("ancestor-or-self") || this.axis.equals("descendant-or-self"))) {
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
				laterString
						.append(this.priorityQuery.toString().substring(a + 1, this.priorityQuery.toString().length()));
				this.priorityQuery = laterString;
			}
		} else {
			this.axisStack.push(this.axis);
			this.axis = "";
		}
    	this.appliesToStack.push(this.lastTable);

    	this.returnValues.push(this.returnValue);
    	this.priorityQueries.push(this.priorityQuery);
    	this.priorityQuery = new StringBuilder();
    	
    	//First step in predicate
    	this.firstStep = true;

    	this.appliesTo = this.lastTable;
    	this.insidePredicate = true;
        	
    }

    @Override
    public void exitPredicate(xpathParser.PredicateContext ctx) {

        System.out.println("exit predicate");
    	this.axis = this.axisStack.pop();
    	
    	this.predicateQuery.insert(0, this.priorityQuery);
    	this.priorityQuery = this.priorityQueries.pop();

    	this.appliesTo = this.appliesToStack.pop();
        this.lastTable = this.appliesTo;
    	this.attributeOnly = false;
    	if (!this.paths.isEmpty()) {
    		//this.paths.pop();
    	}
    	
    	if (this.appliesToStack.size() == 0) {
    		this.insidePredicate = false;
    		//this.axis = "";
    	}
        System.out.println("exit predicate");
    }

    @Override
    public void exitAbbreviatedStep(xpathParser.AbbreviatedStepContext ctx) {
    	StringBuilder g = new StringBuilder();
        g.append(ctx.getChild(0));
        if (g.toString().equals("..")) {
        	
        }
    }
}