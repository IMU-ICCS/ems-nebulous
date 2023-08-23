/*
 * Copyright (C) 2023-2025 Institute of Communication and Computer Systems (imu.iccs.gr)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v2.0.
 * If a copy of the MPL was not distributed with this file, you can obtain one at
 * https://www.mozilla.org/en-US/MPL/2.0/
 */

package eu.nebulous.ems.translate.analyze;

import com.google.gson.Gson;
import eu.nebulous.ems.translate.NebulousEmsTranslatorProperties;
import eu.nebulous.ems.translate.model.MetricModel;
import eu.nebulous.ems.translate.model.MetricModelException;
import eu.nebulous.ems.translate.model.MetricModelNamedElement;
import gr.iccs.imu.ems.translate.TranslationContext;
import gr.iccs.imu.ems.translate.model.*;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class MetricModelAnalyzer {
    private final NebulousEmsTranslatorProperties properties;
    private final Gson gson;

    private List<Sink> EMS_SINKS;

    // ================================================================================================================
    // Model analysis methods

    public void analyzeModel(@NonNull TranslationContext _TC, @NonNull MetricModel metricModel) {
        String modelName = metricModel.getMetricModelName();
        log.debug("MetricModelAnalyzer.analyzeModel(): Analyzing metric model: {}", modelName);

        // Collect metric model components and scopes
        List<MetricModelNamedElement> entries = new ArrayList<>();
        entries.addAll(metricModel.getComponents());
        entries.addAll(metricModel.getScopes());
        Map<String, MetricModelNamedElement> entriesMap = entries.stream()
                .collect(Collectors.toMap(MetricModelNamedElement::getName, x->x));

        List<MetricModelNamedElement> requirements = new ArrayList<>();
        List<MetricModelNamedElement> metrics = new ArrayList<>();

        // Collect Requirements (SLOs, and Opt.Goals))
        entries.forEach(entry -> collectRequirements(entry, _TC, requirements));
        log.debug("MetricModelAnalyzer.analyzeModel(): Collected requirements: {}", requirements);
        log.debug("MetricModelAnalyzer.analyzeModel(): Populated SLO set: {}", _TC.getSLO());

        // Collect Metrics
        entries.forEach(entry -> collectMetrics(entry, _TC, metrics));
        log.debug("MetricModelAnalyzer.analyzeModel(): Collected metrics: {}", metrics);
        log.debug("MetricModelAnalyzer.analyzeModel(): Populated Metric-to-Metric Context map: {}", _TC.getM2MC());

        // Extract MVVs (i.e. constants for Metric Model, variables for CP model)
        entries.forEach(entry -> collectMVVs(entry, _TC, entriesMap));
        log.debug("MetricModelAnalyzer.analyzeModel(): Populated MVV and Composite Metric Variable sets:  mvv={}, composite-metric-vars={}", _TC.getMVV(), _TC.getCompositeMetricVariables());

        // Extract Functions
        entries.forEach(entry -> collectFunctions(entry, _TC));
        log.debug("MetricModelAnalyzer.analyzeModel(): Collected functions: {}", _TC.getFUNC());

        // -- scalability rules --
        // _analyzeScalabilityRules

        // Metric Variables
        entries.forEach(entry -> collectMetricVariables(entry, _TC, metrics));
        log.debug("MetricModelAnalyzer.analyzeModel(): Metric variables: composite-metric-vars={}, raw-metric-vars=** Not displayed **", _TC.getCMVar());

        // Metric Variables Constraints
        // _inferGroupings

        /*String leafGrouping = properties.getLeafNodeGrouping();

        // set full-name pattern in _TC, for full-name generation
        _TC.setFullNamePattern(properties.getFullNamePattern());

        // building Metric-to-Metric Context map
        _buildMetricToMetricContextMap(_TC, metricModel);
        log.debug("MetricModelAnalyzer.analyzeModel(): Populated Metric-to-Metric Context map: {}", _TC.getM2MC());

        // building MVV set
        _buildMetricVariableSets(_TC, metricModel);
        log.debug("MetricModelAnalyzer.analyzeModel():  Populated MVV and Composite Metric Variable sets:  mvv={}, composite-metric-vars={}", _TC.getMVV(), _TC.getCompositeMetricVariables());

        // extract Functions
        _extractFunctions(_TC, metricModel);
        log.debug("MetricModelAnalyzer.analyzeModel(): Extracted functions: {}", _TC.getFUNC());

        // analyze scalability rules
        _analyzeScalabilityRules(_TC, metricModel);
        log.debug("MetricModelAnalyzer.analyzeModel(): Scalability rules: {}", _TC.getE2A());

        // analyze optimisation requirements
        _analyzeOptimisationRequirements(_TC, metricModel);
        log.debug("MetricModelAnalyzer.analyzeModel():  Optimisation Requirements analysis completed");

        // analyze service level objectives
        _analyzeServiceLevelObjectives(_TC, metricModel);
        log.debug("MetricModelAnalyzer.analyzeModel():  Service Level Objectives: {}", _TC.getSLO());

        // analyze metric variables
		_analyzeMetricVariables(_TC, metricModel);
        log.debug("MetricModelAnalyzer.analyzeModel(): Metric variables: composite-metric-vars={}, raw-metric-vars=** Not displayed **", _TC.getCMVar());

        // analyze constraints
//		_analyzeConstraints(_TC, metricModel);
//		log.debug("MetricModelAnalyzer.analyzeModel():  Constraints analysis completed");

        // analyze metric variable constraints (to extract metrics needed in CP model)
		_analyzeMetricVariableConstraints(_TC, metricModel);
		log.debug("MetricModelAnalyzer.analyzeModel():  Metric Variable Constraints analysis completed");

        // infer groupings
        _inferGroupings(_TC, leafGrouping);
        log.debug("MetricModelAnalyzer._inferGroupings():  Grouping inference completed");

        if (log.isTraceEnabled()) {
            _TC.getDAG().traverseDAG(node -> log.trace("------------> DAG node:{}, grouping:{}, id:{}, hash:{}", node, node.getGrouping(), node.getId(), node.hashCode()));
        }*/
        log.debug("MetricModelAnalyzer.analyzeModel(): Analyzing Camel model completed: {}", modelName);
    }

    private void collectRequirements(MetricModelNamedElement entry, TranslationContext _TC, List<MetricModelNamedElement> requirements) {
        log.trace("MetricModelAnalyzer.collectRequirements(): BEGIN: entry={}", entry);
        List<MetricModelNamedElement> slos = entry.getSLOs();
        List<MetricModelNamedElement> ogs = entry.getOptimisationGoals();
        log.trace("MetricModelAnalyzer.collectRequirements(): SLO's: {}", slos);
        log.trace("MetricModelAnalyzer.collectRequirements():  OG's: {}", ogs);
        log.trace("MetricModelAnalyzer.collectRequirements():  requirements-size BEFORE: {}", requirements.size());
        requirements.addAll(slos);
        requirements.addAll(ogs);
        log.trace("MetricModelAnalyzer.collectRequirements():   requirements-size AFTER: {}", requirements.size());

        slos.forEach(slo -> {
            log.trace("MetricModelAnalyzer.collectRequirements(): slos.forEach: slo={}", slo);
            ServiceLevelObjective tcSlo = ServiceLevelObjective.builder()
                    .name(slo.getName())
                    .object(slo)
                    //.constraint(....)
                    .build();
            log.trace("MetricModelAnalyzer.collectRequirements(): slos.forEach: tc-slo={}", tcSlo);
            _TC.addSLO(tcSlo);
        });
    }

    // _buildMetricToMetricContextMap
    private void collectMetrics(MetricModelNamedElement entry, TranslationContext _TC, List<MetricModelNamedElement> metrics) {
        entry.getMetrics().forEach(metric -> {
            metrics.add(metric);
            _TC.addMetricMetricContextPair(
                    Metric.builder().name(entry.getName()).object(entry).build(),
                    MetricContext.builder().name(entry.getName()).object(entry).build());
        });
    }

    //_buildMetricVariableSets
    private void collectMVVs(MetricModelNamedElement entry, TranslationContext _TC, Map<String, MetricModelNamedElement> entriesMap) {
        if (entry.isMetricVariable() && entry.isCurrentConfig()) {
            // Check if it is an MVV or a Metric Variable
            if (entry.isRaw()) {
                log.debug("    Found MVV: {}", entry.getName());
                _TC.addMVV(entry.getName());
            } else {
                throw new MetricModelException("Current-config metric variable is composite: "+entry);
            }

            // Find matching variable for CP model
            String matchingVarName = entry.getFieldValue(MetricModel.METRIC_VARIABLE);

            // Check matching variable
            if (StringUtils.isBlank(matchingVarName))
                throw new MetricModelException("Current-config metric variable has no matching variable: "+entry);
            MetricModelNamedElement matchingVarElement = entriesMap.get(matchingVarName);
            if (matchingVarElement==null)
                throw new MetricModelException("Current-config metric variable's matching variable does not exist: "+entry);
            if (matchingVarElement.getMetricType()!=MetricModelNamedElement.METRIC_TYPE.variable)
                throw new MetricModelException("Current-config metric variable's matching variable is not Metric Variable: "+entry);
            if (StringUtils.isBlank(matchingVarElement.getName()))
                throw new MetricModelException("Current-config metric variable's matching variable has blank name: "+entry);
            if (matchingVarElement.isCurrentConfig())
                throw new MetricModelException("Current-config metric variable's matching variable is also Current-config: "+entry);
            if (! entry.getType().equals( matchingVarElement.getType() ))
                throw new MetricModelException("Current-config metric variable's matching variable has a different type: "+entry);

            // Add MVV-to-Matching var pair in _TC
            MetricVariable matchingMv = MetricVariable.builder()
                    .name(matchingVarElement.getName())
                    .object(matchingVarElement)
                    .build();
            log.trace("    MetricVariable: matchingMv={}", matchingMv);
            log.trace("    MetricVariable: matchingMv={}, mv={}", matchingMv.getName(), entry.getName());
            _TC.getCompositeMetricVariables().put(matchingMv.getName(), entry.getName());
        }
    }

    // _extractFunctions
    private void collectFunctions(MetricModelNamedElement entry, TranslationContext _TC) {
        entry.getFunctions().forEach(func -> {
            // get expression and parameters
            String expression = func.getFieldValue(MetricModel.FUNCTION_EXPRESSION);
            List<String> arguments = func.getArrayAsStringList(MetricModel.FUNCTION_ARGUMENTS);
            log.debug("  Function: {} ::= {} --> {}", func.getName(), expression, arguments);

            // update _TC.FUNC set
            _TC.addFunction(Function.builder()
                    .name(func.getName())
                    .object(func)
                    .expression(expression)
                    .arguments(arguments)
                    .build());
        });
    }

    // _analyzeMetricVariables
    private void collectMetricVariables(MetricModelNamedElement entry, TranslationContext _TC, List<MetricModelNamedElement> metrics) {
        entry.getMetrics().stream()
                .filter(MetricModelNamedElement::isMetricVariable)
                .filter(MetricModelNamedElement::isCurrentConfig)   //XXX:TODO: ????
                .forEach(metricVariable -> {
                    // extract metric variable information
                    String formula = metricVariable.getFormula();
                    List<Metric> componentMetrics = extractFormulaMetrics(formula, metrics);
                    log.trace("    Processing Metric Variable: {}", metricVariable);

                    MetricVariable tcMv = MetricVariable.builder().build();
                    if (! componentMetrics.isEmpty()) {
                        // add metric variable to DAG as top-level node
                        _TC.getDAG().addTopLevelNode(tcMv); //XXX:TODO: Check .setGrouping(getGrouping(tcMv));
                    } else {
                        // if MVV just register it in _TC
                        _TC.addMVV(tcMv);       //XXX:TODO: Duplicate???
                    }

                    // for every component metric
                    componentMetrics.forEach(m -> {
                        // get metric context or variable of current metric
                        Set<MetricContext> ctxSet = _TC.getM2MC().get(m);
                        int ctxSize = (ctxSet == null ? 0 : ctxSet.size());
                        boolean isMVV = _TC.getMVV().contains(m.getName());

                        if (ctxSize == 0 && !isMVV) {
                            log.error("    - No metric context or MVV found for metric '{}' used in metric variable '{}' : ctx-set={}, is-MVV={}",
                                    m.getName(), metricVariable.getName(), ctxSet!=null ? ctxSet.stream().map(NamedElement::getName).toList() : null, isMVV);
                            log.error("      _TC.M2MC: keys: {}", _TC.getM2MC().keySet().stream().map(NamedElement::getName).toList());
                            log.error("      _TC.M2MC: values: {}", _TC.getM2MC().values());
                            log.error("      _TC.MVV: {}", _TC.getMVV());
                            throw new MetricModelException(String.format("No metric context or MVV found for metric '%s' used in the metric variable '%s'",
                                    m.getName(), metricVariable.getName()));
                        } else if (ctxSize > 0 && isMVV || ctxSize > 1) {
                            List<String> ctxNames = ctxSet.stream().map(NamedElement::getName).collect(Collectors.toList());
                            log.error("    - More than one metric contexts and variables were found for metric '{}' used in the metric variable '{}' : ctx-names={}, is-MVV={}",
                                    m.getName(), metricVariable.getName(), ctxNames, isMVV);
                            log.error("      _TC.M2MC: keys: {}", _TC.getM2MC().keySet().stream().map(NamedElement::getName).toList());
                            log.error("      _TC.M2MC: values: {}", _TC.getM2MC().values());
                            log.error("      _TC.MVV: {}", _TC.getMVV());
                            throw new MetricModelException(String.format("More than one metric contexts or MVVs were found for metric '%s' used in the metric variable '%s': ctx-names=%s, is-MVV=%b",
                                    m.getName(), metricVariable.getName(), ctxNames, isMVV));
                        } else if (ctxSize == 1) {
                            MetricContext ctx = ctxSet.iterator().next();

                            // update DAG and decompose metrics
                            if (ctx != null) {
                                // add CTX to DAG
                                _TC.getDAG().addNode(tcMv, ctx);    //XXX:TODO: Check .setGrouping(getGrouping(ctx));

                                // decompose metric context
                      //          _decomposeMetricContext(_TC, ctx);
                            } else {
                                //log.error("  - Metric context for metric '{}' used in the metric variable '{}' is null", m.getName(), mv.getName());
                                throw new MetricModelException(String.format("Metric context for metric '%s' used in the metric variable '%s' is null", m.getName(), metricVariable.getName()));
                            }

                        } else {
                            log.debug("  Component metric is MVV. No DAG node will be added: mvv={}, variable={}", m.getName(), metricVariable.getName());
                        }
                    });


                });
    }

    private List<Metric> extractFormulaMetrics(String formula, List<MetricModelNamedElement> metrics) {
        //XXX:TODO: ++++++++++++++++++
        return Collections.emptyList();
    }






    /*private void _buildMetricToMetricContextMap(CamelTranslationContext _TC, CamelModel camelModel) {
        // extract metric models
        log.debug("  Extracting Metric Type Models from CAMEL model...");
        List<MetricTypeModel> metricModels = camelModel.getMetricModels().stream()
                .filter(mm -> MetricTypeModel.class.isAssignableFrom(mm.getClass()))
                .map(mm -> (MetricTypeModel) mm)
                .collect(Collectors.toList());
        log.debug("  Extracting Metric Type Models from CAMEL model... {}", getListElementNames(metricModels));

        // for every metric type model...
        metricModels.forEach(mm -> {
            // get metric contexts
            log.debug("  Extracting Metric Contexts from Metric Type model {}...", mm.getName());
            EList<MetricContext> contexts = mm.getMetricContexts();
            log.debug("  Extracting Metric Contexts from Metric Type model {}... {}", mm.getName(), getListElementNames(contexts));

            // for every metric context...
            contexts.forEach(mc -> {
                // get metric
                Metric metric = mc.getMetric();
                ObjectContext objContext = mc.getObjectContext();
                log.debug("  Metric-Context: {}.{}.{} == {} --> {}", camelModel.getName(), mm.getName(), mc.getName(), getElementName(metric), getElementName(objContext));

                // update _TC.M2MC map
                _TC.addMetricMetricContextPair(metric, mc);
            });
        });
        log.debug("_buildMetricToMetricContextMap(): M2MC={}", getMapSetElementNames(_TC.getM2MC()));
        //log.debug("_buildMetricToMetricContextMap(): M2MC={}", getMapSetFullNames(_TC, _TC.getM2MC()));
    }

    private void _buildMetricVariableSets(CamelTranslationContext _TC, CamelModel camelModel) {
        // extract metric models
        log.debug("  Extracting Metric Type Models from CAMEL model...");
        List<MetricTypeModel> metricModels = camelModel.getMetricModels().stream()
                .filter(mm -> MetricTypeModel.class.isAssignableFrom(mm.getClass()))
                .map(mm -> (MetricTypeModel) mm)
                .collect(Collectors.toList());
        log.debug("  Extracting Metric Type Models from CAMEL model... {}", getListElementNames(metricModels));

        // for every metric type model...
        metricModels.forEach(mm -> {
            // get current-config metric variables
            log.debug("  Extracting Current-Config Metric Variables from Metric Type model {}... (1)", mm.getName());
            List<MetricVariable> variables = mm.getMetrics().stream()
                    .filter(met -> MetricVariable.class.isAssignableFrom(met.getClass()))
                    .map(met -> (MetricVariable) met)
                    .filter(MetricVariable::isCurrentConfiguration)
                    .filter(mv -> CamelMetadataTool.isFromVariable((MetricVariableImpl) mv))
                    .collect(Collectors.toList());
            log.debug("  Extracting Metric Variables from Metric Type model {}... {}", mm.getName(), getListElementNames(variables));

            // for every metric variable...
            variables.forEach(mv -> {
                // get component metrics
                EList<Metric> componentMetrics = mv.getComponentMetrics();
                Component component = mv.getComponent();
                log.debug("  Metric-Variable: {}.{}.{} :: component-metrics={}, component={}", camelModel.getName(), mm.getName(), mv.getName(), getListElementNames(componentMetrics), getElementName(component));

                // update _TC.MVV set
                if (componentMetrics.size() == 0) {
                    log.debug("  Found MVV: {}.{}.{}", camelModel.getName(), mm.getName(), mv.getName());
                    _TC.addMVV(mv);
                } else
                // ...else update _TC.CMVAR set
                {
                    //XXX:TODO: Possibly unreachable block
                    log.debug("  Found Composite metric variable: {}.{}.{}", camelModel.getName(), mm.getName(), mv.getName());
                    _TC.addCompositeMetricVariable(mv);
                }

                // Find matching variable for CP model
                MetricVariable matchingMv = _findMatchingVar(mv, camelModel);
                log.trace("  MetricVariable: _findMatchingVar: matchingMv = {}", matchingMv);
                if (matchingMv!=null) {
                    log.trace("  MetricVariable: matchingMv={}, mv={}", matchingMv.getName(), mv.getName());
                    _TC.getCompositeMetricVariables().put(matchingMv.getName(), mv.getName());
                }
            });
        });
    }

//XXX:Improve this method (probably pre-process metric models to avoid multiple scans of the model)
    private static MetricVariable _findMatchingVar(MetricVariable mvar, CamelModel camelModel) {
        CamelMetadata type = CamelMetadataTool.findVariableType((MetricVariableImpl) mvar);
        Component comp = mvar.getComponent();
        if (type==null || comp==null) {
            log.warn("  _findMatchingVar: type or component is null: type={}, component={}", type, comp);
            return null;
        }
        String componentName = comp.getName();

        // extract metric models
        List<MetricTypeModel> metricModels = camelModel.getMetricModels().stream()
                .filter(mm -> MetricTypeModel.class.isAssignableFrom(mm.getClass()))
                .map(mm -> (MetricTypeModel) mm)
                .toList();

        // for every metric type model...
        for (MetricTypeModel mm : metricModels) {
            // get metric variables
            log.debug("  Extracting Current-Config Metric Variables from Metric Type model {}... (2)", mm.getName());
            List<MetricVariable> variables = mm.getMetrics().stream()
                    .filter(met -> MetricVariable.class.isAssignableFrom(met.getClass()))
                    .map(met -> (MetricVariable) met)
                    .filter(mv -> ! mv.isCurrentConfiguration())
                    .toList();

            // for every metric variable...
            for (MetricVariable mv : variables) {
                log.debug("_findMatchingVar(): Checking variable: {}, component={}",
                        mv.getName(), getElementName(mv.getComponent()));
                CamelMetadata type1 = CamelMetadataTool.findVariableType((MetricVariableImpl) mv);
                log.debug("_findMatchingVar(): Variable type: {}, type={}", mv.getName(), type1);
                String componentName1 = mv.getComponent().getName();

                if (type1==type && componentName.equals(componentName1)) {
                    log.debug("_findMatchingVar(): Found matching variable: {} -> {}", mvar.getName(), mv.getName());
                    return mv;
                }
                log.trace("_findMatchingVar(): Variable type or component does not match to: search-type={}, search-component={}",
                        type, componentName);
            }
        }

        String mesg = String.format("No matching Metric variable was found for: mv=%s, camel-metadata-type=%s, component=%s", mvar.getName(), type, componentName);
        //log.error(mesg);
        throw new ModelAnalysisException(mesg);
    }

    private void _extractFunctions(CamelTranslationContext _TC, CamelModel camelModel) {
        // extract metric models
        log.debug("  Extracting Metric Type Models from CAMEL model...");
        List<MetricTypeModel> metricModels = camelModel.getMetricModels().stream()
                .filter(mm -> MetricTypeModel.class.isAssignableFrom(mm.getClass()))
                .map(mm -> (MetricTypeModel) mm)
                .collect(Collectors.toList());
        log.debug("  Extracting Metric Type Models from CAMEL model... {}", getListElementNames(metricModels));

        // for every metric type model...
        metricModels.forEach(mm -> {
            // get Function definitions
            log.debug("  Extracting Functions from Metric Type model {}...", mm.getName());
            EList<Function> functions = mm.getFunctions();
            log.debug("  Extracting Functions from Metric Type model {}... {}", mm.getName(), getListElementNames(functions));

            // for every metric context...
            functions.forEach(f -> {
                // get expression and parameters
                String expression = f.getExpression();
                EList<String> arguments = f.getArguments();
                log.debug("  Function: {}.{}.{} == {} --> {}", camelModel.getName(), mm.getName(), f.getName(), expression, arguments);

                // update _TC.FUNC set
                _TC.addFunction(f);
            });
        });
    }

    private void _analyzeScalabilityRules(CamelTranslationContext _TC, CamelModel camelModel) {
        // extract scalability rules
        log.debug("  Extracting Scalability Models from CAMEL model...");
        EList<ScalabilityModel> scalabilityModels = camelModel.getScalabilityModels();
        log.debug("  Extracting Scalability Models from CAMEL model... {}", getListElementNames(scalabilityModels));

        scalabilityModels.forEach(sm -> {
            log.debug("  Extracting Scalability Rules from Scalability model {}...", sm.getName());
            EList<ScalabilityRule> rules = sm.getRules();
            log.debug("  Extracting Scalability Rules from Scalability model {}... {}", sm.getName(), getListElementNames(rules));
            rules.forEach(rule -> {
                String ruleName = rule.getName();
                Event ruleEvent = rule.getEvent();
                EList<Action> ruleActions = rule.getActions();
                log.debug("RULE: {}.{}.{} == {} --> {}", camelModel.getName(), sm.getName(), ruleName, ruleEvent.getName(), ruleActions);

                // add event-action pair to E2A map
                _TC.addEventActionPairs(ruleEvent, ruleActions);
                // add event to DAG as top-level nodes
                _TC.getDAG().addTopLevelNode(ruleEvent).setGrouping(getGrouping(ruleEvent));

                // decompose event
                _decomposeEvent(_TC, ruleEvent);
            });
        });
    }

    private void _analyzeOptimisationRequirements(CamelTranslationContext _TC, CamelModel camelModel) {
        // extract requirement models
        log.debug("  Extracting Requirement Models from CAMEL model...");
        EList<RequirementModel> requirementModels = camelModel.getRequirementModels();
        log.debug("  Extracting Requirement Models from CAMEL model... {}", getListElementNames(requirementModels));

        // for each requirement model...
        requirementModels.forEach(rm -> {
            //List<Requirement> requirements = rm.getRequirements();
            //log.debug("  Extracting Requirements from Requirements model {}... {}", rm.getName(), requirements);

            // extract optimisation requirements
            log.debug("  Extracting Optimisation Requirements from Requirements model {}...", rm.getName());
            List<OptimisationRequirement> optimisations = rm.getRequirements().stream()
                    .filter(req -> OptimisationRequirement.class.isAssignableFrom(req.getClass()))
                    .map(OptimisationRequirement.class::cast)
                    .collect(Collectors.toList());
            log.debug("  Extracting Optimisation Requirements from Requirements model {}... {}", rm.getName(), getListElementNames(optimisations));

            // for each optimisation requirement...
            optimisations.forEach(optr -> {
                // extract metric context and variable
                String reqName = optr.getName();
                MetricContext mc = optr.getMetricContext();
                MetricVariable mv = optr.getMetricVariable();
                log.debug("  Processing Optimisation Requirement {} from Requirements model {}: metric-context={}, metric-variable={}...",
                        reqName, rm.getName(), getElementName(mc), getElementName(mv));

                // Optimisation Goal's metric context's component metrics
                Set<Metric> formulaMetrics = new HashSet<>();
                ObjectContext objCtx;
                if (mc != null) {
                    Metric m = mc.getMetric();
                    objCtx = mc.getObjectContext();
                    log.trace("    Extracting metrics of metric context: metric={}, metric-class={}, component={}", m.getName(), m.getClass().getName(), getComponentName(objCtx));
                    if (m instanceof RawMetric) {
                        formulaMetrics.add(m);
                    } else if (m instanceof CompositeMetric) {
                        formulaMetrics.addAll(_extractMetricsFromFormula(_TC, ((CompositeMetric) m).getFormula()));
                    } else if (m instanceof MetricVariable) {
                        formulaMetrics.addAll(_extractMetricsFromFormula(_TC, ((MetricVariable) m).getFormula()));
                    }
                }

                // Optimisation Goal's metric variable's component metrics
                if (mv != null) {
                    log.trace("    Extracting metrics of metric variable: variable={}", mv.getName());
                    log.trace("    Extracting metrics of metric variable:  formula={}", mv.getFormula());
                    log.trace("    Extracting metrics of metric variable: formula-metrics={}", formulaMetrics);
                    formulaMetrics.addAll(_extractMetricsFromFormula(_TC, mv.getFormula()));
                }

                // update DAG and decompose metrics and variables
                for (Metric m : formulaMetrics) {
                    if (m instanceof MetricVariable) {
                        log.trace("    Processing component metric variable of opt. goal formula: goal={}, variable={}, formula={}", reqName, m.getName(), ((MetricVariable) m).getFormula());

                        // add variable to DAG as top-level node
                        _TC.getDAG().addTopLevelNode(m).setGrouping(getGrouping(m));

                        // decompose metric
                        _decomposeMetricVariable(_TC, (MetricVariable) m);
                    } else {
                        String formula = (m instanceof CompositeMetric) ? ((CompositeMetric) m).getFormula() : null;
                        log.trace("    Processing component metric context of opt. goal formula: goal={}, context={}, formula={}", reqName, mv.getName(), formula);

                        // get metric context for metric
                        Set<MetricContext> mctx = _TC.getM2MC(m);
                        if (mctx.size() != 1) {
                            String mesg = String.format("Metric in formula has 0 or more than one metric contexts: metric=%s, formula=%s, contexts=%s", m.getName(), formula, getSetElementNames(mctx));
                            log.error("    Error while processing Optimisation Goal: opt-goal={}, req-model={}: {}", reqName, rm.getName(), mesg);
                            throw new ModelAnalysisException(mesg);
                        }
                        MetricContext mc_1 = mctx.iterator().next();

                        // add metric context to DAG as top-level node
                        _TC.getDAG().addTopLevelNode( m ).setGrouping(getGrouping(m));

                        // add metric context to DAG
                        _TC.getDAG().addNode(m, mc_1).setGrouping(getGrouping(mc_1));

                        // decompose metric context
                        _decomposeMetricContext(_TC, mc_1);
                    }
                }
            });
        });
    }

    private Set<Metric> _extractMetricsFromFormula(CamelTranslationContext _TC, String formula) {
        log.debug("    Extracting metrics from formula: {}", formula);
        Set<String> argNames = MathUtil.getFormulaArguments(formula);
        log.debug("    Formula arguments: {}", argNames);

        // find formula component metrics
        Set<Metric> formulaMetrics = _TC.getM2MC().keySet().stream()
                .filter(m -> argNames.contains(m.getName()))
                .map(m -> m.getObject(Metric.class))
                .collect(Collectors.toSet());
        log.debug("    Formula metrics: {}", getSetElementNames(formulaMetrics));

        // find formula component metric variables
        Set<Metric> formulaVars = _TC.getCMVar_1().stream()
                .filter(mv -> argNames.contains(mv.getName()))
                .map(m -> m.getObject(Metric.class))
                .collect(Collectors.toSet());
        log.debug("    Formula variables: {}", getSetElementNames(formulaVars));

        // merge results
        formulaMetrics.addAll(formulaVars);
        log.debug("    Formula metrics and variables: {}", getSetElementNames(formulaMetrics));

        return formulaMetrics;
    }

    private void _analyzeServiceLevelObjectives(CamelTranslationContext _TC, CamelModel camelModel) {
        // extract requirement models
        log.debug("  Extracting Requirement Models (for SLO) from CAMEL model...");
        EList<RequirementModel> requirementModels = camelModel.getRequirementModels();
        log.debug("  Extracting Requirement Models (for SLO) from CAMEL model... {}", getListElementNames(requirementModels));

        // for each requirement model...
        requirementModels.forEach(rm -> {
            // extract Service Level Objectives
            log.debug("  Extracting Service Level Objectives from Requirements model {}...", rm.getName());
            List<ServiceLevelObjective> slos = rm.getRequirements().stream()
                    .filter(req -> ServiceLevelObjective.class.isAssignableFrom(req.getClass()))
                    .map(ServiceLevelObjective.class::cast)
                    .collect(Collectors.toList());
            log.debug("  Extracting Service Level Objectives from Requirements model {}... {}", rm.getName(), getListElementNames(slos));

            // for each Service Level Objective...
            slos.forEach(slo -> {
                // extract metric context and variable
                String sloName = slo.getName();
                Constraint sloConstr = slo.getConstraint();
                Event sloEvent = slo.getViolationEvent();
                log.debug("  Processing Service Level Objective {} from Requirements model {}: constraint={}, violation-event={}...",
                        sloName, rm.getName(), getElementName(sloConstr), getElementName(sloEvent));

                // add SLO to SLO set
                _TC.addSLO(slo);
                // add event to DAG as top-level nodes
                _TC.getDAG().addTopLevelNode(slo).setGrouping(getGrouping(slo));

                // update DAG and decompose metric constraint
                if (sloConstr != null) {
                    // add SLO constraint to DAG
                    _TC.getDAG().addNode(slo, sloConstr).setGrouping(getGrouping(sloConstr));

                    // decompose constraint
                    _decomposeConstraint(_TC, sloConstr);
                }

                // Nothing to do with SLO violation event
            });
        });
    }

    private void _analyzeMetricVariables(CamelTranslationContext _TC, CamelModel camelModel) {
        // extract requirement models
        log.debug("  Extracting Metric Type Models from CAMEL model...");
        List<MetricTypeModel> metricModels = camelModel.getMetricModels().stream()
                .filter(mm -> MetricTypeModel.class.isAssignableFrom(mm.getClass()))
                .map(mm -> (MetricTypeModel) mm)
                .collect(Collectors.toList());
        log.debug("  Extracting Metric Type Models from CAMEL model... {}", getListElementNames(metricModels));

        // for every metric type model...
        metricModels.forEach(mm -> {
            // get metric variables
            log.debug("  Extracting current-config Metric Variables from Metric Type model {}... (3)", mm.getName());
            List<MetricVariable> variables = mm.getMetrics().stream()
                    .filter(met -> MetricVariable.class.isAssignableFrom(met.getClass()))
                    .map(met -> (MetricVariable) met)
                    .filter(MetricVariable::isCurrentConfiguration)
                    .collect(Collectors.toList());
            log.debug("  Extracting current-config Metric Variables from Metric Type model {}... {}", mm.getName(), getListElementNames(variables));

            // for every metric variable...
            variables.forEach(mv -> {
                // extract metric variable information
                String mvName = mv.getName();
                MetricTemplate template = mv.getMetricTemplate();
                boolean isCurrConfig = mv.isCurrentConfiguration();
                boolean isOnNodeCand = mv.isOnNodeCandidates();
                Component component = mv.getComponent();
                String formula = mv.getFormula();
                List<Metric> componentMetrics = ListUtils.emptyIfNull(mv.getComponentMetrics());
                boolean containsMetrics = ! componentMetrics.isEmpty();
                log.debug("  Processing Metric Variable {} from Metric Type model {}: template={}, is-current-configuration={}, is-on-node-candidates={}, component={}, formula={}, component-metrics={}, contains-metrics={}...",
                        mvName, mm.getName(), getElementName(template), isCurrConfig, isOnNodeCand, getElementName(component), formula, getListElementNames(componentMetrics), containsMetrics);

                if ((formula == null || formula.trim().isEmpty()) && containsMetrics) {
                    log.error("  Metric Variable has component metrics but formula IS EMPTY: {}", mvName);
                    throw new ModelAnalysisException(String.format("Metric Variable has component metrics but formula IS EMPTY: %s", mvName));
                }
                if (formula != null && !formula.trim().isEmpty() && !containsMetrics) {
                    log.warn("  Metric Variable has NO component metrics: name={}, formula={}", mvName, formula);
                }

                _checkFormulaAndComponents(_TC, formula, componentMetrics);

                if (componentMetrics.size() > 0) {
                    // add metric variable to DAG as top-level node
                    _TC.getDAG().addTopLevelNode(mv).setGrouping(getGrouping(mv));
                } else {
                    // if MVV just register it in _TC
                    _TC.addMVV(mv);
                }

                // for every component metric
                componentMetrics.forEach(m -> {
                    // get metric context or variable of current metric
                    Set<MetricContext> ctxSet = _TC.getM2MC(m);
                    int ctxSize = (ctxSet == null ? 0 : ctxSet.size());
                    boolean isMVV = _TC.getMVV().contains(m.getName());

                    if (ctxSize == 0 && !isMVV) {
                        log.error("  - No metric context or variable found for metric '{}' used in the metric variable '{}' : ctx-set={}, is-MVV={}", m.getName(), mv.getName(), getSetElementNames(ctxSet), isMVV);
                        log.error("  _TC.M2MC: keys: {}", getSetElementNames(_TC.getM2MC().keySet()));
                        log.error("  _TC.M2MC: values: {}", _TC.getM2MC().values());
                        log.error("  _TC.MVV: {}", getSetElementNames(_TC.getMVV()));
                        throw new ModelAnalysisException(String.format("No metric context or MVV found for metric '%s' used in the metric variable '%s'", m.getName(), mv.getName()));
                    } else if (ctxSize > 0 && isMVV || ctxSize > 1) {
                        List<String> ctxNames = ctxSet.stream().map(NamedElement::getName).collect(Collectors.toList());
                        log.error("  - More than one metric contexts and variables were found for metric '{}' used in the metric variable '{}' : ctx-names={}, is-MVV={}", m.getName(), mv.getName(), ctxNames, isMVV);
                        log.error("  _TC.M2MC: keys: {}", getSetElementNames(_TC.getM2MC().keySet()));
                        log.error("  _TC.M2MC: values: {}", _TC.getM2MC().values());
                        log.error("  _TC.MVV: {}", getSetElementNames(_TC.getMVV()));
                        throw new ModelAnalysisException(String.format("More than one metric contexts and MVVs were found for metric '%s' used in the metric variable '%s': ctx-names=%s, is-MVV=%b", m.getName(), mv.getName(), ctxNames, isMVV));
                    } else if (ctxSize == 1) {
                        MetricContext ctx = ctxSet.iterator().next();

                        // update DAG and decompose metrics
                        if (ctx != null) {
                            // add CTX to DAG
                            _TC.getDAG().addNode(mv, ctx).setGrouping(getGrouping(ctx));

                            // decompose metric context
                            _decomposeMetricContext(_TC, ctx);

                        } else {
                            //log.error("  - Metric context for metric '{}' used in the metric variable '{}' is null", m.getName(), mv.getName());
                            throw new ModelAnalysisException(String.format("Metric context for metric '%s' used in the metric variable '%s' is null", m.getName(), mv.getName()));
                        }

                    } else {
                        log.debug("  Component metric is MVV. No DAG node will be added: mvv={}, variable={}", m.getName(), mv.getName());
                    }
                });
            });
        });
    }

    *//*private void _analyzeConstraints(CamelTranslationContext _TC, CamelModel camelModel) {
        // extract constraint models
        log.debug("  Extracting Constraint Models from CAMEL model...");
        EList<ConstraintModel> constraintModels = camelModel.getConstraintModels();
        log.debug("  Extracting Constraint Models from CAMEL model... {}", getListElementNames(constraintModels));

        // for each constraint model...
        constraintModels.forEach(cm -> {
            // extract constraints
            log.debug("  Extracting Constraints from Constraints model {}...", cm.getName());
            List<Constraint> constraints = new ArrayList<>(cm.getConstraints());
            log.debug("  Extracting Constraints from Constraints model {}... {}", cm.getName(), getListElementNames(constraints));

            // for each Constraint...
            constraints.forEach(con -> {
                // extract metric context and variable
                String conName = con.getName();
                log.debug("  Processing Constraint {} from Constraints model {}: ...", con.getName(), cm.getName());

                // add constraint to DAG
                _TC.getDAG().addTopLevelNode(con).setGrouping(getGrouping(con));

                // decompose constraint
                _decomposeConstraint(_TC, con);
            });
        });
    }*//*

    private void _analyzeMetricVariableConstraints(CamelTranslationContext _TC, CamelModel camelModel) {
        // extract constraint models
        log.debug("  Extracting Constraint Models from CAMEL model...");
        EList<ConstraintModel> constraintModels = camelModel.getConstraintModels();
        log.debug("  Extracting Constraint Models from CAMEL model... {}", getListElementNames(constraintModels));

        // for each constraint model...
        constraintModels.forEach(cm -> {
            // extract constraints
            log.debug("  Extracting Metric Variable Constraints from Constraints model {}...", cm.getName());
            List<MetricVariableConstraint> constraints = cm.getConstraints().stream()
                    .filter(c -> c instanceof MetricVariableConstraint)
                    .map(c -> (MetricVariableConstraint)c)
                    .collect(Collectors.toList());
            log.debug("  Extracting Metric Variable Constraints from Constraints model {}... {}", cm.getName(), getListElementNames(constraints));

            // for each Constraint...
            constraints.forEach(con -> {
                // extract constraint name and metric variable
                String conName = con.getName();
                log.debug("  Processing Metric Variable Constraint {} from Constraints model {}: ...", conName, cm.getName());

                MetricVariable mv = con.getMetricVariable();
                log.debug("  Metric Variable Constraint {}: metric variable: {}", con.getName(), mv.getName());

                // decompose constraint
                DAGNode mvNode = _decomposeMetricVariableConstraint(_TC, con, true);

                // Remove top-level metric variable and make its children top-level nodes
                Set<DAGNode> children = _TC.getDAG().getNodeChildren(mvNode);
                for (DAGNode child : children) {
                    _TC.getDAG().removeEdge(mvNode, child);
                    //_TC.DAG.addEdge(_TC.DAG.getRootNode().getElement(), child.getElement());
                    _TC.getDAG().addTopLevelNode(child.getElement());
                }
                _TC.getDAG().removeNode(mvNode.getElement());
            });
        });
    }

    private void _decomposeEvent(CamelTranslationContext _TC, Event event) {
        log.debug("  _decomposeEvent(): {} :: {}", event.getName(), event.getClass().getName());

        // decompose event
        if (event instanceof BinaryEventPattern be) {
            String op = be.getOperator().getName();
            Event lEvent = be.getLeftEvent();
            Event rEvent = be.getRightEvent();
            log.debug("  _decomposeEvent(): BinaryEventPattern: {} ==> {} {} {} ", be.getName(), lEvent.getName(), op, rEvent.getName());

            log.debug("  _decomposeEvent(): Adding left event to DAG: {}, parent={}", lEvent.getName(), be.getName());
            _TC.getDAG().addNode(be, lEvent).setGrouping(getGrouping(lEvent));
            log.debug("  _decomposeEvent(): Adding right event to DAG: {}, parent={}", rEvent.getName(), be.getName());
            _TC.getDAG().addNode(be, rEvent).setGrouping(getGrouping(rEvent));

            log.debug("  _decomposeEvent(): Decomposing left event: {}", lEvent.getName());
            _decomposeEvent(_TC, lEvent);
            log.debug("  _decomposeEvent(): Decomposing right event: {}", rEvent.getName());
            _decomposeEvent(_TC, rEvent);
        } else if (event instanceof UnaryEventPattern ue) {
            String op = ue.getOperator().getName();
            Event uEvent = ue.getEvent();
            log.debug("  _decomposeEvent(): UnaryEventPattern: {} ==> {} {} ", ue.getName(), op, uEvent.getName());

            _TC.getDAG().addNode(event, uEvent).setGrouping(getGrouping(uEvent));

            _decomposeEvent(_TC, uEvent);
        } else if (event instanceof NonFunctionalEvent nfe) {
            MetricConstraint constr = nfe.getMetricConstraint();
            boolean isViolation = nfe.isIsViolation();
            log.debug("  _decomposeEvent(): NonFunctionalEvent: {} ==> {}{} ", nfe.getName(), isViolation ? "VIOLATION OF " : "", constr.getName());

            _TC.getDAG().addNode(event, constr).setGrouping(getGrouping(constr));

            _decomposeMetricConstraint(_TC, constr);
        } else {
            throw new ModelAnalysisException(String.format("Invalid event type occurred: %s  class=%s", event.getName(), event.getClass().getName()));
        }
    }

    private void _decomposeConstraint(CamelTranslationContext _TC, Constraint constraint) {
        log.debug("  _decomposeConstraint(): {} :: {}", constraint.getName(), constraint.getClass().getName());
        if (MetricConstraint.class.isAssignableFrom(constraint.getClass())) {
            log.debug("  _decomposeConstraint(): Metric Constraint found: {}", constraint.getName());
            _decomposeMetricConstraint(_TC, (MetricConstraint) constraint);
        } else if (IfThenConstraint.class.isAssignableFrom(constraint.getClass())) {
            log.debug("  _decomposeConstraint(): If-Then Constraint found: {}", constraint.getName());
            _decomposeIfThenConstraint(_TC, (IfThenConstraint) constraint);
        } else if (MetricVariableConstraint.class.isAssignableFrom(constraint.getClass())) {
            log.debug("  _decomposeConstraint(): Metric Variable Constraint found: {}", constraint.getName());
            // Not used in EMS
            //_decomposeMetricVariableConstraint(_TC, (MetricVariableConstraint)constraint);
        } else if (LogicalConstraint.class.isAssignableFrom(constraint.getClass())) {
            log.debug("  _decomposeConstraint(): Logical Constraint found: {}", constraint.getName());
            _decomposeLogicalConstraint(_TC, (LogicalConstraint) constraint);
        } else {
            throw new ModelAnalysisException(String.format("Invalid Constraint type occurred: %s  class=%s", constraint.getName(), constraint.getClass().getName()));
        }
    }

    private void _decomposeMetricConstraint(CamelTranslationContext _TC, MetricConstraint constraint) {
        log.debug("  _decomposeMetricConstraint(): {} :: {}", constraint.getName(), constraint.getClass().getName());
        Date validity = constraint.getValidity();
        String op = constraint.getComparisonOperator().getName();
        double threshold = constraint.getThreshold();
        MetricContext context = constraint.getMetricContext();
        log.debug("  _decomposeMetricConstraint(): {} ==> {} {} {}  validity: {}", constraint.getName(), context.getName(), op, threshold, validity);

        _TC.getDAG().addNode(constraint, context).setGrouping(getGrouping(context));

        // cache constraint
        _TC.addMetricConstraint(constraint);

        _decomposeMetricContext(_TC, context);
    }

    private DAGNode _decomposeMetricVariableConstraint(CamelTranslationContext _TC, MetricVariableConstraint constraint, boolean isTopLevel) {
        log.debug("  _decomposeMetricVariableConstraint(): {} :: {}", constraint.getName(), constraint.getClass().getName());
        Date validity = constraint.getValidity();
        String op = constraint.getComparisonOperator().getName();
        double threshold = constraint.getThreshold();
        MetricVariable mvar = constraint.getMetricVariable();
        log.debug("  _decomposeMetricVariableConstraint(): {} ==> {} {} {}  validity: {}", constraint.getName(), mvar.getName(), op, threshold, validity);

        DAGNode mvNode;
        if (isTopLevel)
            mvNode = _TC.getDAG().addTopLevelNode(mvar).setGrouping(Grouping.GLOBAL);
        else
            mvNode = _TC.getDAG().addNode(constraint, mvar).setGrouping(Grouping.GLOBAL);

        // cache constraint
        _TC.addMetricVariableConstraint(constraint);

        log.trace("  _decomposeMetricVariableConstraint(): CMVAR: {}", _TC.getCMVar());
        log.trace("  _decomposeMetricVariableConstraint(): MVV:   {}", _TC.getMVV());
        _decomposeMetricVariable(_TC, mvar);

        return mvNode;
    }

    private void _decomposeIfThenConstraint(CamelTranslationContext _TC, IfThenConstraint constraint) {
        log.debug("  _decomposeIfThenConstraint(): {} :: {}", constraint.getName(), constraint.getClass().getName());
        Constraint ifConstraint = constraint.getIf();
        Constraint thenConstraint = constraint.getThen();
        Constraint elseConstraint = constraint.getElse();
        log.debug("  _decomposeIfThenConstraint(): {} ==> if: {}, then: {}, else: {}",
                constraint.getName(), getElementName(ifConstraint), getElementName(thenConstraint), getElementName(elseConstraint));

        _TC.getDAG().addNode(constraint, ifConstraint).setGrouping(getGrouping(ifConstraint));
        _TC.getDAG().addNode(constraint, thenConstraint).setGrouping(getGrouping(thenConstraint));
        if (elseConstraint!=null)
            _TC.getDAG().addNode(constraint, elseConstraint).setGrouping(getGrouping(elseConstraint));

        // cache constraint
        _TC.addIfThenConstraint(constraint);

        _decomposeConstraint(_TC, ifConstraint);
        _decomposeConstraint(_TC, thenConstraint);
        if (elseConstraint!=null)
            _decomposeConstraint(_TC, elseConstraint);
    }

    private void _decomposeLogicalConstraint(CamelTranslationContext _TC, LogicalConstraint constraint) {
        log.debug("  _decomposeLogicalConstraint(): {} :: {}", constraint.getName(), constraint.getClass().getName());
        EList<Constraint> componentConstraints = constraint.getConstraints();
        LogicalOperatorType operator = constraint.getLogicalOperator();
        log.debug("  _decomposeLogicalConstraint(): {} ==> operator: {}, component-constraints: {}", constraint.getName(), operator.getName(), getListElementNames(componentConstraints));

        List<DAGNode> nodeList = componentConstraints.stream()
                .map(lc -> _TC.getDAG().addNode(constraint, lc).setGrouping(getGrouping(lc)))
                .collect(Collectors.toList());

        // cache constraint
        _TC.addLogicalConstraint(constraint, nodeList);

        componentConstraints.forEach(lc -> _decomposeConstraint(_TC, lc) );
    }

    private boolean _decomposeMetricVariable(CamelTranslationContext _TC, MetricVariable mvar) {
        log.debug("  _decomposeMetricVariable(): {} :: {}", mvar.getName(), mvar.getClass().getName());

        // Get Metric Variable parameters
        MetricTemplate template = mvar.getMetricTemplate();
        boolean currentConfig = mvar.isCurrentConfiguration();
        boolean nodeCandidates = mvar.isOnNodeCandidates();
        Component component = mvar.getComponent();
        String formula = mvar.getFormula();
        EList<Metric> metrics = mvar.getComponentMetrics();
        log.debug("  _decomposeMetricVariable(): {} :: template={}, current-config={}, on-node-candidates={}, component={}, formula={}, component-metrics={}",
                mvar.getName(), template.getName(), currentConfig, nodeCandidates, getElementName(component), formula, getListElementNames(metrics));

        _checkFormulaAndComponents(_TC, formula, metrics);

        // for each component Metric...
        boolean hasNonMVVComponents = false;    // ?? does any measurable metric exist in component metric??
        for (Metric m : metrics) {
            // check if it is a composite or raw metric
            if (CompositeMetric.class.isAssignableFrom(m.getClass()) || RawMetric.class.isAssignableFrom(m.getClass())) {
                Set<MetricContext> contexts = _TC.getM2MC(m);
                if (contexts == null)
                    throw new ModelAnalysisException(String.format("Metric variable %s: Component metric not found in M2MC map: %s", mvar.getName(), m.getName()));
                //if (contexts==null) { log.warn("  _decomposeMetricVariable(): Metric not found in M2MC map. Ignoring: name={}", m.getName()); return false; }
                if (contexts.size() > 1)
                    throw new ModelAnalysisException(String.format("Metric variable %s: Component metric has >1 metric contexts in M2MC map: metric=%s, contexts=%s", mvar.getName(), m.getName(), contexts));
                if (contexts.size() == 1) {
                    hasNonMVVComponents = true;

                    // add metric context as mvar's child and decompose metric context
                    MetricContext mc = contexts.iterator().next();
                    log.debug("  _decomposeMetricVariable(): {} :: Component metric with exactly one metric context found: metric={}, context={}", mvar.getName(), m.getName(), mc.getName());
                    _TC.getDAG().addNode(mvar, mc).setGrouping(getGrouping(mc));
                    _decomposeMetricContext(_TC, mc);
                }
            } else
            // check if it is metric variable
            if (m instanceof MetricVariable) {
                // check if it is a composite metric variable
                if (_TC.getCMVar().contains(m.getName())) {
                    hasNonMVVComponents = true;

                    // add metric variable 'm' as mvar's child and decompose it
                    MetricVariable mv = (MetricVariable) m;
                    log.debug("  _decomposeMetricVariable(): {} :: Component composite metric variable found: {}", mvar.getName(), mv.getName());
                    _TC.getDAG().addNode(mvar, mv).setGrouping(getGrouping(mv));
                    //if (_decomposeMetricVariable(_TC, mv)) hasNonMVVComponents = true;
                    _decomposeMetricVariable(_TC, mv);
                } else
                // check if it is an MVV
                if (_TC.getMVV().contains(m.getName())) {
                    log.debug("  _decomposeMetricVariable(): {} :: Component MVV found: {}", mvar.getName(), m.getName());
                    _TC.getDAG().addNode(mvar, m).setGrouping(getGrouping(m));
                    // MVV can be pruned later (if property 'translator.prune-mmv' is true)
                } else
                // check if it is a CP model variable (i.e. solver variable)
                if (_isCpModelVariable(_TC, (MetricVariable)m)) {
                    log.debug("  _decomposeMetricVariable(): {} :: CP model variable encountered: {}", mvar.getName(), m.getName());
                    // No DAG node is added for CP model variables
                } else {
                    throw new ModelAnalysisException(String.format("INTERNAL ERROR: Metric Variable not found in CMVAR or in MVV sets and is not a CP model variable: %s, class=%s", m.getName(), m.getClass().getName()));
                }
            } else {
                throw new ModelAnalysisException(String.format("Invalid Metric type occurred: %s, class=%s", m.getName(), m.getClass().getName()));
            }
        }

        return hasNonMVVComponents;
    }

    private void _decomposeMetricContext(CamelTranslationContext _TC, MetricContext context) {
        log.debug("  _decomposeMetricContext(): {} :: {}", context.getName(), context.getClass().getName());

        // Get common Metric Context parameters
        Metric metric = context.getMetric();
        Schedule schedule = context.getSchedule();
        ObjectContext objContext = context.getObjectContext();
        log.debug("  _decomposeMetricContext(): common fields: {} :: metric={}, schedule={}, object={}",
                context.getName(), metric.getName(), getElementName(schedule), getElementName(objContext));

        // Commented addition in DAG and decomposition of Metrics
        *//*_TC.DAG.addNode(context, metric).setGrouping(getGrouping(metric));

        _decomposeMetric(_TC, metric, objContext);*//*

        if (context instanceof CompositeMetricContext cmc) {
            Window window = cmc.getWindow();
            GroupingType grouping = cmc.getGroupingType();
            EList<MetricContext> composingMetricContexts = cmc.getComposingMetricContexts();
            log.debug("  _decomposeMetricContext(): CompositeMetricContext: {} :: window={}, grouping={}, composing-metric-contexts={}",
                    cmc.getName(), getElementName(window), grouping != null ? grouping.getName() : null, getListElementNames(composingMetricContexts));

            for (MetricContext mctx : composingMetricContexts) {
                _TC.getDAG().addNode(context, mctx).setGrouping(getGrouping(mctx));

                _decomposeMetricContext(_TC, mctx);
            }
        } else if (context instanceof RawMetricContext rmc) {
            Sensor sensor = rmc.getSensor();
            log.debug("  _decomposeMetricContext(): RawMetricContext: {} :: sensor={}", rmc.getName(), getElementName(sensor));

            DAGNode sensorDagNode = _TC.getDAG().addNode(context, sensor).setGrouping(getGrouping(sensor));
            _TC.addMonitorsForSensor(sensor.getName(), _createMonitorsForSensor(_TC, objContext, sensor, sensorDagNode));

            _processSensor(_TC, sensor, objContext);
        } else {
            throw new ModelAnalysisException(String.format("Invalid Metric Context type occurred: %s  class=%s", context.getName(), context.getClass().getName()));
        }

        // Check if it is a LOAD-annotated metric
        log.trace("  _decomposeMetricContext(): Checking if it is a LOAD METRIC: metric={}", metric.getName());
        log.trace("  _decomposeMetricContext(): LOAD METRIC annotation: {}", properties.getLoadMetricAnnotation());
        if (StringUtils.isNotBlank(properties.getLoadMetricAnnotation())) {
            if (hasAnnotation(metric, properties.getLoadMetricAnnotation().trim())) {
                log.trace("  _decomposeMetricContext(): It is a LOAD METRIC: metric-name={}, context-name={}, element-name={}",
                        metric.getName(), context.getName(), getElementName(context));
                _TC.addLoadAnnotatedMetric(getElementName(context));
                log.trace("  _decomposeMetricContext(): Updated LOAD METRICS set: {}", _TC.getLoadAnnotatedMetricsSet());

                // Also add connection to LOAD metrics root node
                log.trace("  _decomposeMetricContext(): Adding a new Metric Variable for LOAD METRIC topic: context-name={}", context.getName());
                String newMvName =
                        String.format(properties.getLoadMetricVariableFormatter(), metric.getName());
                LoadMetricVariable newMv = new LoadMetricVariable(newMvName, context);
                newMv.setMetricTemplate(metric.getMetricTemplate());
                log.debug("  _decomposeMetricContext(): New LOAD Metric Variable: {}", newMv.getName());

                _TC.addElementToNamePair(newMv, newMvName);
                DAGNode newMvNode = _TC.getDAG().addTopLevelNode(newMv).setGrouping(getGrouping(newMv));
                log.trace("  _decomposeMetricContext(): Added LOAD Metric Variable as a TOP-LEVEL DAG node: {}", newMvNode);

                newMv.getComponentMetrics().add(context.getMetric());
                log.trace("  _decomposeMetricContext(): Added LOAD Metric as component metric of LOAD Metric Variable: context-name={}, metric-name={}, variable-name={}",
                        context.getName(), metric.getName(), newMv.getName());

                _TC.getDAG().addNode(newMv, context).setGrouping(getGrouping(context));
                log.trace("  _decomposeMetricContext(): Added LOAD Metric Context under (new) LOAD Metric Variable: context-name={}, variable-name={}",
                        context.getName(), newMv.getName());
            } else {
                log.debug("  _decomposeMetricContext(): No LOAD METRIC annotation found in metric: {}", metric.getName());
            }
        } else {
            log.debug("  _decomposeMetricContext(): LOAD METRIC annotation not set in configuration");
        }
    }

    private boolean hasAnnotation(NamedElement elem, String annotation) {
        log.trace("  hasAnnotation: BEGIN: elem={}, looking-for-annotation={}", elem.getName(), annotation);
        if (elem.getAnnotations()==null || elem.getAnnotations().size()==0) return false;
        return elem.getAnnotations().stream().anyMatch(ann -> {
            try {
                log.trace("  hasAnnotation:   Checking Annotation: id={}, name={}", ann.getId(), ann.getName());
                //StringBuilder annPath = new StringBuilder(ann.getName());
                StringBuilder annPath = new StringBuilder(ann.getId());
                camel.mms.MmsConcept p;
                if (ann instanceof camel.mms.MmsConceptInstance || ann instanceof camel.mms.MmsProperty) {
                    p = (camel.mms.MmsConcept) ann.eContainer();
                    log.trace("  hasAnnotation:  Adding {} parent:   id={}, name={}", ann.getClass().getSimpleName(), p.getId(), p.getName());
                    //annPath.insert(0, p.getName() + ".");
                    annPath.insert(0, p.getId() + ".");
                } else {
                    p = (camel.mms.MmsConcept) ann;
                }
                while (p.getParent() != null) {
                    p = p.getParent();
                    log.trace("  hasAnnotation:  Adding parent:   id={}, name={}", p.getId(), p.getName());
                    //annPath.insert(0, p.getName() + ".");
                    annPath.insert(0, p.getId() + ".");
                }
                log.trace("  hasAnnotation: Annotation: {}", annPath);
                log.trace("  hasAnnotation: Annotation matches to looking-for-annotation: {}", annPath.toString().equals(annotation));
                return annPath.toString().equals(annotation);
            } catch (Exception e) {
                log.error("  hasAnnotation: Annotation: EXCEPTION: elem={}, looking-for-annotation={}", elem.getName(), annotation);
                throw e;
            }
        });
    }

    private void _decomposeMetric(CamelTranslationContext _TC, Metric metric, ObjectContext objContext) {
        log.debug("  _decomposeMetric(): metric={}, metric-class={}, component={}", metric.getName(), metric.getClass().getName(), getComponentName(objContext));

        // Get common Metric parameters
        MetricTemplate template = metric.getMetricTemplate();
        log.debug("  _decomposeMetric(): Common fields of metric {}: template={}", metric.getName(), template.getName());

        // Uncomment to include templates in the DAG and Topics set
        //_TC.getDAG().addNode(metric, template).setGrouping( getGrouping(template) );
        //_decomposeMetricTemplate(_TC, template, objContext);

        if (metric instanceof CompositeMetric cm) {
            String formula = cm.getFormula();
            EList<Metric> componentMetrics = cm.getComponentMetrics();
            log.debug("  _decomposeMetric(): CompositeMetric: metric={}, formula={}, component-metrics={}",
                    cm.getName(), formula, getListElementNames(componentMetrics));

            _checkFormulaAndComponents(_TC, formula, componentMetrics);

            for (Metric m : componentMetrics) {
                _TC.getDAG().addNode(metric, m).setGrouping(getGrouping(m));

                _decomposeMetric(_TC, m, objContext);
            }
        } else if (metric instanceof RawMetric rm) {
            log.debug("  _decomposeMetric(): RawMetric: metric={}", rm.getName());
        } else if (metric instanceof MetricVariable mv) {
            log.debug("  _decomposeMetric(): MetricVariable: variable={}", mv.getName());

            _decomposeMetricVariable(_TC, mv);
        } else {
            throw new ModelAnalysisException(String.format("Invalid Metric type occurred: %s  class=%s",
                    metric.getName(), metric.getClass().getName()));
        }
    }

    private void _decomposeMetricTemplate(CamelTranslationContext _TC, MetricTemplate template, ObjectContext objContext) {
        log.debug("  _decomposeMetricTemplate(): {} :: {} for {}", template.getName(), template.getClass().getName(), getComponentName(objContext));

        ValueType valType = template.getValueType();
        int direction = template.getValueDirection();
        Unit unit = template.getUnit();
        MeasurableAttribute attribute = template.getAttribute();
        EList<Sensor> sensors = attribute.getSensors();
        log.debug("  _decomposeMetricTemplate(): {} :: {} {}/{} {} -- Sensors: {}",
                template.getName(), attribute.getName(), getElementName(valType), direction, getElementName(unit), getListElementNames(sensors));

        for (Sensor s : sensors) {
            DAGNode sensorDagNode = _TC.getDAG().addNode(template, s).setGrouping(getGrouping(s));
            _TC.addMonitorsForSensor(s.getName(), _createMonitorsForSensor(_TC, objContext, s, sensorDagNode));

            _processSensor(_TC, s, objContext);
        }
    }

    private void _processSensor(CamelTranslationContext _TC, Sensor sensor, ObjectContext objContext) {
        log.debug("    _processSensor(): {} :: {} for {}", sensor.getName(), sensor.getClass().getName(), getComponentName(objContext));

        String configStr = sensor.getConfiguration();
        boolean push = sensor.isIsPush();
        log.debug("    _processSensor(): {} :: push={}, configuration={}", sensor.getName(), push, configStr);

        _TC.addComponentSensorPair(objContext, sensor);
    }

    private synchronized void _initializeSinks() {
        if (EMS_SINKS == null) {
            log.debug("    _initializeSinks(): Active Sinks type: {}", properties.getSinks());
            log.debug("    _initializeSinks(): Sink type configurations: {}", properties.getSinkConfig());

            List<Sink> sinks = new ArrayList<>();
            for (String sinkType : CollectionUtils.emptyIfNull(properties.getSinks())) {
                log.trace("    _initializeSinks(): Processing sink type: {}", sinkType);
                Sink.Type sinkTypeType = Sink.Type.valueOf(sinkType);
                Map<String,String> configMap = properties.getSinkConfig().get(sinkType);

                if (MapUtils.isEmpty(configMap)) {
                    log.warn("    _initializeSinks(): WARN: Missing configuration for sink type: {}", sinkType);
                    continue;
                }

                // Create configuration for sink type
                Map<String, String> sinkTypeConfig = new HashMap<>(configMap);

                log.debug("    _initializeSinks(): {} sink type configuration: {}", sinkType, sinkTypeConfig);

                // Create sink entry
                Sink sink = Sink.builder()
                        .type(sinkTypeType)
                        .configuration(sinkTypeConfig)
                        .build();
                sinks.add(sink);
            }

            // Store sink configurations
            if (sinks.size()>0)
                EMS_SINKS = sinks;
            log.debug("    _initializeSinks(): Sink type configurations initialized");
        }
    }

    private Set<Monitor> _createMonitorsForSensor(CamelTranslationContext _TC, ObjectContext objContext, Sensor sensor, DAGNode sensorDagNode) {
        log.debug("    _createMonitorsForSensor(): sensor={}", sensor.getName());

        // Check if sensor monitors have already been created
        if (_TC.containsMonitorsForSensor(sensor.getName())) {
            log.debug("    _createMonitorsForSensor(): sensor={} :: Monitors for this sensor have already been added", sensor.getName());
            return Collections.emptySet();
        }

        // Get push or pull sensor (configured)
        Sensor monitorSensor = _createPushOrPullSensor(sensor);

        // Get monitor component
        String monitorComponent = getComponentName(objContext);

        // Initialize JMS_SINK if needed
        if (EMS_SINKS == null) {
            _initializeSinks();
        }

        // Create results set
        Set<Monitor> results = new HashSet<>();
        for (DAGNode parent : _TC.getDAG().getParentNodes(sensorDagNode)) {
            // Get metric name from sensor
            log.debug("    + _createMonitorsForSensor(): sensor={} :: parent-node={}", sensor.getName(), parent.getName());
            RawMetricContext rmc = parent.getElement().getObject(RawMetricContext.class);
            log.debug("    + _createMonitorsForSensor(): sensor={} :: context={}", sensor.getName(), rmc.getName());
            *//*Metric metric = rmc.getMetric();
            String monitorMetric = metric.getName();
            log.debug("    + _createMonitorsForSensor(): sensor={} :: metric={}, component={}", sensor.getName(), monitorMetric, monitorComponent);*//*
            String monitorMetric = sensor.getName();
            log.debug("    + _createMonitorsForSensor(): sensor={} :: metric/topic={}, component={}", sensor.getName(), monitorMetric, monitorComponent);

            // Create a Monitor instance
            Monitor monitor = Monitor.builder()
                    .metric(monitorMetric)
                    .sensor(monitorSensor)
                    .component(monitorComponent)
                    .sinks(EMS_SINKS)
                    .build();
            // watermark will be set in Coordinator

            results.add(monitor);
        }

        log.debug("    _createMonitorsForSensor(): sensor={} :: monitors={}", sensor.getName(), results);

        return results;
    }

    private Sensor _createPushOrPullSensor(Sensor sensor) {
        log.debug("    _createPushOrPullSensor(): BEGIN: sensor={} : {}", sensor.getName(), sensor);

        Map<String, String> sensorConfigMap;

        // Get configuration from JSON-string (if Sensor is annotated as JSON-formatted string)
        sensorConfigMap = getSensorConfigurationFromJsonString(sensor);

        // Get configuration from sensor attributes (will override settings from configuration string)
        Map<String, String> attributesConfigMap = getSensorConfigurationFromAttributes(sensor);

        log.debug("    _createPushOrPullSensor(): sensor={} :: sensorConfigMap BEFORE merging with attribute config.: {}", sensor.getName(), sensorConfigMap);
        if (sensorConfigMap != null && attributesConfigMap != null)
            sensorConfigMap.putAll(attributesConfigMap);
        else if (sensorConfigMap == null)
            sensorConfigMap = attributesConfigMap;
        log.debug("    _createPushOrPullSensor(): sensor={} :: sensorConfigMap AFTER merging with attribute config.: {}", sensor.getName(), sensorConfigMap);

        // Process sensor configuration
        int port = -1;
        String className = null;
        Map<String,String> sensorConfig;
        Interval interval = null;

        if (sensorConfigMap!=null && sensorConfigMap.size()>0) {
            // Extract sensor settings from configuration Map
            try {
                if (sensor.isIsPush()) {
                    // Push sensor config - Get port
                    String portStr = StrUtil.getWithVariations(sensorConfigMap, "port", "").trim();
                    port = StrUtil.strToInt(portStr, -1, (i)->i>0 && i<=65535, false,
                            String.format("    _createPushOrPullSensor(): ERROR: Invalid port. Using -1: sensor=%s, configuration=%s\n",
                                    sensor.getName(), sensorConfigMap));
                } else {
                    // Pull Sensor config - Get class name
                    className = StrUtil.getWithVariations(sensorConfigMap, "className", "").trim();

                    // Pull Sensor config - Get interval period
                    String periodStr = StrUtil.getWithVariations(sensorConfigMap, "intervalPeriod", "").trim();
                    int period = StrUtil.strToInt(periodStr, (int)properties.getSensorDefaultInterval(), (i)->i>=properties.getSensorMinInterval(), false,
                            String.format("    _createPushOrPullSensor(): Invalid interval period in configuration: sensor=%s, configuration=%s\n",
                                    sensor.getName(), sensorConfigMap));

                    // Pull Sensor config - Get interval unit
                    String periodUnitStr = StrUtil.getWithVariations(sensorConfigMap, "intervalUnit", "").trim();
                    Interval.UnitType periodUnit = StrUtil.strToEnum(periodUnitStr, Interval.UnitType.class, Interval.UnitType.SECONDS, false,
                            String.format("    _createPushOrPullSensor(): Invalid interval unit in configuration: sensor=%s, configuration=%s\n",
                                    sensor.getName(), sensorConfigMap));

                    // Create an Interval instance
                    interval = Interval.builder()
                            .period(period)
                            .unit(periodUnit)
                            .build();
                }

            } catch (Exception e) {
                log.error("    _createPushOrPullSensor(): ERROR: While processing sensor configuration: sensor={}, configuration={}\n",
                        sensor.getName(), sensor.getConfiguration(), e);
                throw e;
            }

        } else {
            // Sensor is neither annotated as a JSON-formatted-configuration element
            // nor there are any attributes. Hence, config. Map is empty
            log.debug("    _createPushOrPullSensor(): Sensor configuration is empty or missing: sensor={}, config={}, attributes={}",
                    sensor.getName(), sensor.getConfiguration(), sensor.getAttributes());

            if (sensor.isIsPush()) {
                String portStr = sensor.getConfiguration();
                port = StrUtil.strToInt(portStr, -1, (i)->i>0 && i<=65535, false,
                        String.format("    _createPushOrPullSensor(): ERROR: Invalid port. Using -1: sensor=%s, port=%s\n", sensor.getName(), portStr));
            } else {
                className = sensor.getConfiguration();
                interval = Interval.builder()
                        .period((int) properties.getSensorDefaultInterval())
                        .unit(Interval.UnitType.SECONDS)
                        .build();
            }
        }

        // Build a list of KeyValuePair's using the configuration Map
        sensorConfig = Collections.emptyMap();
        if (sensorConfigMap!=null) {
            sensorConfig = sensorConfigMap;
        }

        // Create PushSensor or PullSensor
        Sensor pushOrPullSensor;
        if (sensor.isIsPush()) {
            log.debug("    _createPushOrPullSensor(): PUSH sensor: sensor={}", sensor.getName());
            PushSensor pushSensor = new PushSensor();
            pushSensor.setPort(port);
//XXX: BUG: It generates the 'configuration' field as a sibling of 'port'; not under the 'additionalProperties'
//            pushSensor.setAdditionalProperties(Collections.singletonMap("configuration", sensorConfig));
            pushOrPullSensor = pushSensor;
            log.debug("    _createPushOrPullSensor(): sensor={} :: port={}, additional-properties={}, PushSensor: {}",
                    sensor.getName(), port, pushSensor.getAdditionalProperties(), pushSensor);
        } else {
            log.debug("    _createPushOrPullSensor(): PULL sensor: sensor={}", sensor.getName());
            PullSensor pullSensor = new PullSensor();
            pullSensor.setClassName(className);
            pullSensor.setConfiguration(sensorConfig);
            pullSensor.setInterval(interval);
            pushOrPullSensor = pullSensor;
            log.debug("    _createPushOrPullSensor(): sensor={} :: class-name={}, PullSensor: {}",
                    sensor.getName(), className, pullSensor);
        }
        return pushOrPullSensor;
    }

    private Map<String, String> getSensorConfigurationFromAttributes(Sensor sensor) {
        // Process sensor attributes. Attributes will override configuration string settings
        List<Attribute> sensorAttributes = sensor.getAttributes();
        if (sensorAttributes!=null) {
            log.debug("    getSensorConfigurationFromAttributes(): sensor={} :: Processing attributes: {}", sensor.getName(), sensorAttributes);
            Map<String, String> attributesConfigMap =
                    sensorAttributes.stream().filter(Objects::nonNull)
                            .collect(Collectors.toMap(
                                    NamedElement::getName,
                                    attribute -> {
                                        if (attribute.getValue() instanceof StringValue)
                                            return ((StringValue) attribute.getValue()).getValue();
                                        else if (attribute.getValue() instanceof BooleanValue)
                                            return Boolean.toString(((BooleanValue) attribute.getValue()).isValue());
                                        else if (attribute.getValue() instanceof IntValue)
                                            return Integer.toString(((IntValue) attribute.getValue()).getValue());
                                        else if (attribute.getValue() instanceof FloatValue)
                                            return Float.toString(((FloatValue) attribute.getValue()).getValue());
                                        else if (attribute.getValue() instanceof DoubleValue)
                                            return Double.toString(((DoubleValue) attribute.getValue()).getValue());
                                        else
                                            throw new ModelAnalysisException("Invalid Attribute Value type: " + attribute.getValue().getClass().getName() + " in sensor configuration: sensor=" + sensor.getName() + ", attribute=" + attribute.getName());
                                    }
                            ));
            log.debug("    getSensorConfigurationFromAttributes(): sensor={} :: Configuration extracted from attributes (will override config. string settings): {}", sensor.getName(), attributesConfigMap);
            return attributesConfigMap;
        }
        return null;
    }

    private Map<String, String> getSensorConfigurationFromJsonString(Sensor sensor) {
        // Check if sensor is annotated as a JSON-formatted-configuration element (Annotation is configurable)
        // If the configured annotation is set to '*' then even non-annotated sensors will be treated like they
        // were annotated
        Map<String, String> sensorConfigMap = null;
        if ("*".equals(properties.getSensorConfigurationAnnotation()) ||
                hasAnnotation(sensor, properties.getSensorConfigurationAnnotation()))
        {
            log.debug("    _createPushOrPullSensor(): Sensor configuration string is in JSON format: sensor={}, config={}", sensor.getName(), sensor.getConfiguration());

            if (StringUtils.isNotBlank(sensor.getConfiguration())) {
                // Convert JSON-formatted configuration string to Map
                try {
                    // Convert JSON string to Map
                    Type type = new TypeToken<Map<Object, Object>>(){}.getType();
                    Map<Object, Object> map = gson.fromJson(sensor.getConfiguration(), type);

                    // Convert Map to Map<String,String>
                    sensorConfigMap = new LinkedHashMap<>();
                    for (Map.Entry<Object, Object> e : map.entrySet()) {
                        if (e.getKey()!=null) {
                            sensorConfigMap.put(
                                    e.getKey().toString(),
                                    e.getValue()!=null ? e.getValue().toString() : null);
                        }
                    }
                    log.debug("    _createPushOrPullSensor(): Extracted sensor configuration: sensor={}, configuration={}",
                            sensor.getName(), sensorConfigMap);
                } catch (Exception e) {
                    log.error("    _createPushOrPullSensor(): ERROR: Sensor configuration does not contain a valid JSON string: sensor={}, configuration={}\n",
                            sensor.getName(), sensor.getConfiguration(), e);
                    throw e;
                }
            } else {
                log.error("    _createPushOrPullSensor(): ERROR: Sensor configuration string is blank. It must contain configuration in JSON format: sensor={}, configuration={}",
                        sensor.getName(), sensor.getConfiguration());
                throw new IllegalArgumentException("Sensor configuration string is blank. It must contain configuration in JSON format: sensor="+ sensor.getName()+", configuration="+ sensor.getConfiguration());
            }
        }
        return sensorConfigMap;
    }

    private void _checkFormulaAndComponents(CamelTranslationContext _TC, String formula, List<Metric> componentMetrics) {
        if (!properties.isFormulaCheckEnabled()) return;
        if (StringUtils.isBlank(formula)) return;

        if (componentMetrics == null) componentMetrics = new ArrayList<>();
        List<String> metricNames = getListElementNames(componentMetrics);
        log.debug("    _checkFormulaAndComponents(): formula={}, component-metrics={}", formula, metricNames);
        Set<String> argNames = MathUtil.getFormulaArguments(formula);
        log.debug("    _checkFormulaAndComponents(): formula={}, arguments={}", formula, argNames);

        // check if all arguments are found in component metrics - Detailed report
        Set<String> diff1 = new HashSet<>(argNames);
        metricNames.forEach(diff1::remove);
        log.debug("    _checkFormulaAndComponents(): diff1={}", diff1);
        if (diff1.size() > 0) {
            log.error("    _checkFormulaAndComponents(): ERROR: Formula arguments not found in component metrics: formula={}, arguments-not-found={}, component-metrics={}", formula, diff1, metricNames);
        }

        // check if all component metrics are found in arguments - Detailed report
        Set<String> diff2 = new HashSet<>(metricNames);
        argNames.forEach(diff2::remove);
        log.debug("    _checkFormulaAndComponents(): diff2={}", diff2);
        if (diff2.size() > 0) {
            log.error("    _checkFormulaAndComponents(): ERROR: Formula component metrics not found in formula arguments: formula={}, metrics-not-found={}, arguments={}", formula, diff2, argNames);
        }

        // if there are differences throw an exception
        if (diff1.size() > 0 || diff2.size() > 0) {
            String message = String.format("Formula arguments and component metrics do not match: formula=%s, component-metrics=%s, arguments=%s", formula, metricNames, argNames);
            log.error("    _checkFormulaAndComponents(): ERROR: {}", message);
            throw new ModelAnalysisException(message);
        }

        // check metrics against contexts
        for (Metric m : componentMetrics) {
            log.trace("    _checkFormulaAndComponents(): Checking formula component metric: formula={}, metric={}", formula, m.getName());

            // check if it is a composite or raw metric
            if (CompositeMetric.class.isAssignableFrom(m.getClass()) || RawMetric.class.isAssignableFrom(m.getClass())) {
                Set<MetricContext> contexts = _TC.getM2MC(m);
                String message = null;
                if (contexts == null)
                    message = String.format("Formula component metric does not have a metric context in M2MC map: formula=%s, metric=%s", formula, m.getName());
                else if (contexts.size() > 1)
                    message = String.format("Formula component metric has >1 metric contexts in M2MC map: formula=%s, metric=%s, contexts=%s", formula, m.getName(), contexts);
                if (message != null) {
                    log.error("    _checkFormulaAndComponents(): ERROR: {}", message);
                    log.error("    _checkFormulaAndComponents(): ERROR: metric: {}, hash: {}", m, m.hashCode());
                    log.error("    _checkFormulaAndComponents(): ERROR: M2MC: {}", _TC.getM2MC());
                    throw new ModelAnalysisException(message);
                }

                if (log.isTraceEnabled()) {
                    assert contexts != null;
                    log.trace("    _checkFormulaAndComponents(): Formula component metric has exactly 1 metric context in M2MC map: formula={}, metric={}, context={}", formula, m.getName(), contexts.iterator().next());
                }
            } else
            // check if it is metric variable
            if (MetricVariable.class.isAssignableFrom(m.getClass())) {
                // check if it is a composite metric variable
                if (_TC.getCMVar().contains(m.getName())) {
                    if (log.isTraceEnabled()) {
                        log.trace("    _checkFormulaAndComponents(): Formula composite component metric variable found: formula={}, metric-variable={}", formula, m.getName());
                    }
                } else
                // check if it is an MVV
                if (_TC.getMVV().contains(m.getName())) {
                    if (log.isTraceEnabled()) {
                        log.trace("    _checkFormulaAndComponents(): Formula component MVV found: formula={}, mvv={}", formula, m.getName());
                    }
                } else
                // check if it is a CP model variable (i.e. solver variable)
                if (_isCpModelVariable(_TC, (MetricVariable)m)) {
                    if (log.isTraceEnabled()) {
                        log.trace("    _checkFormulaAndComponents(): CP model variable encountered: formula={}, cp-model-var={}", formula, m.getName());
                    }
                } else {
                    String message = String.format("INTERNAL ERROR: Formula component metric variable not found in CMVAR or in MVV sets and it is not CP model variable: formula=%s, metric-variable=%s", formula, m.getName());
                    log.error("    _checkFormulaAndComponents(): {}", message);
                    throw new ModelAnalysisException(message);
                }
            } else {
                String message = String.format("INTERNAL ERROR: Invalid formula component metric: formula=%s, metric=%s, metric-class=%s", formula, m.getName(), m.getClass().getName());
                log.error("    _checkFormulaAndComponents(): {}", message);
                throw new ModelAnalysisException(message);
            }
        }

        log.trace("    _checkFormulaAndComponents(): Formula arguments and component metrics match: formula={}, arguments={}, component-metric={}", formula, argNames, metricNames);
    }

    private boolean _isCpModelVariable(CamelTranslationContext _TC, MetricVariable mv) {
        log.debug("    _isCpModelVariable: mv={}", getElementName(mv));
        boolean result = CamelMetadataTool.isFromVariable((MetricVariableImpl) mv);
        log.debug("    _isCpModelVariable: result={}", result);
        return result;
    }

    // ================================================================================================================
    // Helper methods

    private static String getElementName(NamedElement elem) {
        return elem != null ? elem.getName() : null;
    }

    private static String getElementName(NamedElement elem) {
        return elem != null ? elem.getName() : null;
    }

    private List<String> getListElementNames(List<?> list) {
        List<String> names = new ArrayList<>();
        for (Object elem : ListUtils.emptyIfNull(list)) {
            if (elem instanceof NamedElement) {
                names.add(((NamedElement) elem).getName());
            }
            if (elem instanceof NamedElement namedElement) {
                names.add(namedElement.getName());
            }
        }
        return names;
    }

    private Set<String> getSetElementNames(Set set) {
        if (set == null) return Collections.emptySet();
        HashSet<String> names = new HashSet<>();
        for (Object elem : set) {
            if (elem instanceof NamedElement) {
                names.add(((NamedElement) elem).getName());
            }
            if (elem instanceof NamedElement namedElement) {
                names.add(namedElement.getName());
            }
        }
        return names;
    }

    private Map<String, Set<String>> getMapSetElementNames(Map map) {
        if (map == null) return Collections.emptyMap();
        Map<String, Set<String>> results = new HashMap<>();
        for (Object key : map.keySet()) {
            Object value = map.get(key); //entry.getValue();
            if (key instanceof NamedElement && value instanceof Set) {
                results.put(((NamedElement) key).getName(), getSetElementNames((Set) value));
            }
            if (key instanceof NamedElement namedElement && value instanceof Set valueSet) {
                results.put(namedElement.getName(), getSetElementNames(valueSet));
            }
        }
        return results;
    }

	*//*private Map<String,Set<String>> getMapSetFullNames(CamelTranslationContext _TC, Map map) {
		if (map==null) return null;
		HashMap<String,Set<String>> results = new HashMap<>();
		for (Object key : map.keySet()) {
			Object value = map.get(key); //entry.getValue();
			if (key instanceof NamedElement && value instanceof Set) {
				String keyStr = _TC.E2N.get((NamedElement)key);
				Set<String> newSet = new HashSet<>();
				for (Object item : (Set)value) {
					if (item instanceof NamedElement) {
						newSet.add( _TC.E2N.get((NamedElement)item) );
					}
				}
				results.put(keyStr, newSet);
			}
		}
		return results;
	}*//*

    private String getComponentName(ObjectContext objContext) {
        if (objContext == null) return null;
        Component comp = objContext.getComponent();
        Data data = objContext.getData();
        if (comp != null && data != null)
            throw new ModelAnalysisException("Invalid Object Context: properties Component and Data cannot be not null at the same time: " + objContext.getName());
        if (comp != null) return comp.getName();
        if (data != null) return data.getName();
        throw new ModelAnalysisException("Invalid Object Context: either Component or Data property must be not null: " + objContext.getName());
    }

    private boolean checkIfUpperwareElement(NamedElement elem) {
        return (elem instanceof MetricVariable)
                || (elem instanceof ServiceLevelObjective)
                || (elem instanceof Event)
                || (elem instanceof Constraint)
                ;
    }

    private Grouping getGrouping(NamedElement elem) {
        // Upperware nodes are always GLOBAL
        if (checkIfUpperwareElement(elem)) {
            return Grouping.GLOBAL;
        }
        // Deduce CMC grouping from component groupings
        if (elem instanceof CompositeMetricContext cmc) {
            GroupingType grouping = cmc.getGroupingType();
            return Grouping.valueOf(grouping.getName());
        }
        return Grouping.UNSPECIFIED;
    }

    // ================================================================================================================
    // Grouping inference methods

    private void _inferGroupings(CamelTranslationContext _TC, String leafGrouping) {
        log.debug("  _inferGroupings(): Inferring DAG node groupings...");

        // traverse DAG bottom-up
        Set<DAGNode> leafs = _TC.getDAG().getLeafNodes();
        log.debug("  _inferGroupings(): DAG Leaf Nodes: {}", leafs);

        Grouping grouping = Grouping.valueOf(leafGrouping);
        for (DAGNode node : leafs) {
            log.debug("    ----> leaf node: element class: {}", node.getElement().getClass());

            // Upperware nodes are always GLOBAL
            if (checkIfUpperwareElement(node.getElement().getObject(NamedElement.class))) {
                node.setGrouping(Grouping.GLOBAL);
            } else
            // else use leaf grouping
            {
                node.setGrouping(grouping);
            }

            _inferAncestorGroupings(_TC, node);
        }
    }

    private void _inferAncestorGroupings(CamelTranslationContext _TC, DAGNode node) {
        log.debug("  _inferAncestorGroupings(): Inferring parent groupings of DAG node: {}...", node);

        // Get child node grouping
        Grouping childGrouping = node.getGrouping();
        log.debug("  _inferAncestorGroupings(): DAG node grouping: {}...", childGrouping);
        if (childGrouping == null || childGrouping.equals(Grouping.UNSPECIFIED)) {
            throw new IllegalArgumentException("_inferAncestorGroupings: Node passed has null or UNSPECIFIED grouping: " + node.getName() + ", grouping=" + childGrouping);
        }

        // process node parents
        Set<DAGNode> parents = _TC.getDAG().getParentNodes(node);
        log.debug("    ----> parent nodes: {}", parents);
        DAGNode _root = _TC.getDAG().getRootNode();
        for (DAGNode parent : parents) {
            // exclude DAG root from further processing
            if (parent == _root) {
                // ...unless it is top-level node and its grouping is not GLOBAL
                if (childGrouping != Grouping.GLOBAL) {
                    // ...then add to it a (new) top-level, parent node with GLOBAL grouping
                    NamedElement elem = node.getElement().getObject(NamedElement.class);
                    DAGNode newParent = _TC.getDAG().addTopLevelNode(elem, "DUPL_" + node.getName()).setGrouping(Grouping.GLOBAL);
                    _TC.getDAG().addEdge(newParent.getName(), node.getName());
                    _TC.getDAG().removeEdge(_root, node);
                }
                continue;
            }

            Grouping parentGrouping = parent.getGrouping();
            log.debug("    ----> parent: {} with grouping: {} lower-than-child-grouping={}", parent, parentGrouping, parentGrouping != null ? parentGrouping.lowerThan(Grouping.UNSPECIFIED) : "n/a");
            if (parentGrouping == null || parentGrouping.equals(Grouping.UNSPECIFIED) || parentGrouping.lowerThan(childGrouping)) {
                Grouping newGrouping;

                // Upperware nodes are always GLOBAL
                if (checkIfUpperwareElement(parent.getElement().getObject(NamedElement.class))) {
                    newGrouping = Grouping.GLOBAL;
                } else
                // else use child grouping
                {
                    //XXX:TODO: The following is not completely correct. Check if an aggregation operator is involved etc etc.
                    newGrouping = childGrouping;
                }
                log.debug("    ----> setting parent grouping: {} grouping: {}, id: {}, hash: {}", parent, newGrouping, parent.getId(), parent.hashCode());
                parent.setGrouping(newGrouping);
            }

            // recursively process ancestors
            _inferAncestorGroupings(_TC, parent);
        }
    }

    public static class LoadMetricVariable extends MetricVariableImpl {
        public LoadMetricVariable(String name, MetricContext context) {
            setName(name);
            setMetricContext(context);
        }
    }*/
}