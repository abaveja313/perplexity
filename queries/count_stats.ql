import java

class TargetFile extends File {
  TargetFile() {
    this.isJavaSourceFile() and
    this.getRelativePath() = "{{ relative_path }}"
  }
}

predicate hasKnownPackage(Callable c) {
  c.getDeclaringType().getPackage().getName() in [
    // Java Builtin
    "java.lang%", "java.util%", "java.io%", "java.net%", "java.math%", "java.time%",
    // JSON Libraries
    "org.json%", "com.google.code.json",
    // Logging Libraries 
    "org.apache.logging%", "org.slf4j",
    // Common Libraries
    "com.fasterxml.jackson", "gnu.trove", "org.apache.commons%", "com.google.guava%", "org.joda.time%", "org.eclipse%"
  ]
}

predicate contains(Method target, MethodCall call) {
  call.getCaller() = target
  or
  call.getCaller().getEnclosingCallable() = target
}

int getClassOtherInvocations(Method targetMethod) {
  result = count(MethodCall mCall |
    contains(targetMethod, mCall) and
    mCall.getCallee().getDeclaringType() = targetMethod.getDeclaringType()
  )
}

int getNonClassUnknownInvocations(Method targetMethod) {
  result = count(MethodCall mCall |
    contains(targetMethod, mCall) and
    mCall.getCallee().getDeclaringType() != targetMethod.getDeclaringType() and
    not hasKnownPackage(mCall.getCallee())
  )
}

int getNonClassKnownInvocations(Method targetMethod) {
  result = count(MethodCall mCall |
    contains(targetMethod, mCall) and
    mCall.getCallee().getDeclaringType() != targetMethod.getDeclaringType() and
    hasKnownPackage(mCall.getCallee())
  )
}

int numLambdaAndHigherOrderExpr(Method m) {
  result = count(Expr e | e.getEnclosingCallable() = m and e instanceof FunctionalExpr)
}

int numFieldAndPropertyExpr(Method m) {
  result = count(Expr e | e.getEnclosingCallable() = m and
    (e instanceof PropertyRefExpr or
     e instanceof RecordBindingVariableExpr or
     e instanceof RecordPatternExpr or
     e instanceof VirtualMethodCall)
  )
}

int numControlFlowExpr(Method m) {
  result = count(Expr e | e.getEnclosingCallable() = m and
    (e instanceof ConditionalExpr or
     e instanceof ChooseExpr)
  )
}

int numLiteralExpr(Method m) {
  result = count(Expr e | e.getEnclosingCallable() = m and
    (e instanceof CompileTimeConstantExpr or
     e instanceof StringTemplateExpr)
  )
}

int numExplicitTypeCasts(Method m) {
  result = count(Expr e | e.getEnclosingCallable() = m and
    (e instanceof CastExpr or
     e instanceof WildcardTypeAccess or
     e instanceof UnionTypeAccess or
     e instanceof TypeAccess or
     e instanceof IntersectionTypeAccess)
  )
}

int numImplicitTypeCasts(Method m) {
  result = count(Expr e | e.getEnclosingCallable() = m and
    (e instanceof ImplicitCastExpr or
     e instanceof ImplicitCoercionToUnitExpr or
     e instanceof ImplicitNotNullExpr or
     e instanceof UnsafeCoerceExpr)
  )
}

int numValDiscardExpr(Method m) {
  result = count(ValueDiscardingExpr e | e.getEnclosingCallable() = m)
}

int numAssignExpr(Method m) {
  result = count(Expr e | e.getEnclosingCallable() = m and
    (e instanceof AssignAndExpr or
     e instanceof AssignLeftShiftExpr or
     e instanceof AssignOrExpr or
     e instanceof AssignRemExpr or
     e instanceof AssignRightShiftExpr or
     e instanceof AssignUnsignedRightShiftExpr or
     e instanceof AssignXorExpr or
     e instanceof AssignDivExpr or
     e instanceof AssignMulExpr)
  )
}

int numBitwiseExpr(Method m) {
  result = count(Expr e | e.getEnclosingCallable() = m and
    (e instanceof AndBitwiseExpr or
     e instanceof BitNotExpr or
     e instanceof BitwiseExpr or
     e instanceof LeftShiftExpr or
     e instanceof OrBitwiseExpr or
     e instanceof RightShiftExpr or
     e instanceof UnsignedRightShiftExpr or
     e instanceof XorBitwiseExpr)
  )
}

from Method m, TargetFile targetFile
where m.fromSource() and
      m.getDeclaringType().hasName("{{ class_name }}") and
      m.hasName("{{ method_name }}") and
      m.getFile() = targetFile
select count(Field f | m.accesses(f) and f.fromSource()) as field_accesses,
       count(Field f | m.writes(f) and f.fromSource()) as field_writes,
       getClassOtherInvocations(m) as same_class_other_invoc,
       getNonClassKnownInvocations(m) as diff_class_known_invoc,
       getNonClassUnknownInvocations(m) as diff_class_unknown_invoc,
       numLambdaAndHigherOrderExpr(m) as lambda_higher_order_count,
       numFieldAndPropertyExpr(m) as field_property_count,
       numControlFlowExpr(m) as control_flow_count,
       numLiteralExpr(m) as literal_count,
       numExplicitTypeCasts(m) as explicit_cast_count,
       numImplicitTypeCasts(m) as implicit_cast_count,
       numValDiscardExpr(m) as val_disc_expr_count,
       numAssignExpr(m) as assign_expr_count,
       numBitwiseExpr(m) as bitwise_expr_count,
       m.getMetrics().getHalsteadLength() as halstead_length,
       m.getMetrics().getHalsteadVocabulary() as halstead_vocab,
       m.getMetrics().getCyclomaticComplexity() as cyclomatic_complexity,
       m.getMetrics().getEfferentCoupling() as efferent_coupling,
       m.getMetrics().getAfferentCoupling() as afferent_coupling,
       m.getDeclaringType().getMetrics().getMaintainabilityIndex() as maintainability_index,
       m.getDeclaringType().getMetrics().getMaintainabilityIndexWithoutComments() as maintainability_index_no_comments,
       m.getMetrics().getNumberOfParameters() as num_params,
       m.getMetrics().getNumberOfLinesOfCode() as num_lines_code,
       m.getStringSignature() as gsig
