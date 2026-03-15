from __future__ import annotations

from fin_ai_auditor.domain.models import (
    AuditFinding,
    AuditLocation,
    AuditRun,
    ConfluencePatchPreview,
    ConfluencePageUpdateDetails,
    JiraTicketAICodingBrief,
)


def build_confluence_update_details(
    *,
    page_title: str,
    page_url: str,
    changed_sections: list[str],
    change_summary: list[str],
    page_id: str | None = None,
    applied_revision_id: str | None = None,
    execution_mode: str | None = None,
    patch_preview: ConfluencePatchPreview | None = None,
) -> ConfluencePageUpdateDetails:
    return ConfluencePageUpdateDetails(
        page_title=page_title,
        page_url=page_url,
        changed_sections=_dedupe_preserve_order(changed_sections),
        change_summary=_dedupe_preserve_order(change_summary),
        page_id=page_id,
        applied_revision_id=applied_revision_id,
        execution_mode=execution_mode,
        patch_preview=patch_preview,
    )


def build_jira_ticket_brief(
    *,
    run: AuditRun,
    ticket_key: str = "PENDING",
    ticket_url: str | None = None,
    findings: list[AuditFinding],
) -> JiraTicketAICodingBrief:
    selected_findings = findings[:4]
    title = _build_ticket_title(findings=selected_findings)
    affected_parts = _build_affected_parts(findings=selected_findings)
    evidence = _build_evidence(findings=selected_findings)
    correction_measures = _build_correction_measures(findings=selected_findings)
    target_state = _build_target_state(findings=selected_findings)
    acceptance_criteria = _build_acceptance_criteria(findings=selected_findings, run=run)
    implications = _build_implications(findings=selected_findings)
    implementation_notes = _build_implementation_notes(run=run, findings=selected_findings)
    validation_steps = _build_validation_steps(findings=selected_findings)
    problem_description = _build_problem_description(findings=selected_findings)
    reason = _build_reason(findings=selected_findings)
    ai_coding_prompt = _build_ai_coding_prompt(
        run=run,
        title=title,
        problem_description=problem_description,
        reason=reason,
        correction_measures=correction_measures,
        target_state=target_state,
        acceptance_criteria=acceptance_criteria,
        implications=implications,
        affected_parts=affected_parts,
        evidence=evidence,
        implementation_notes=implementation_notes,
        validation_steps=validation_steps,
    )
    return JiraTicketAICodingBrief(
        ticket_key=ticket_key,
        ticket_url=ticket_url,
        title=title,
        problem_description=problem_description,
        reason=reason,
        correction_measures=correction_measures,
        target_state=target_state,
        acceptance_criteria=acceptance_criteria,
        implications=implications,
        affected_parts=affected_parts,
        evidence=evidence,
        implementation_notes=implementation_notes,
        validation_steps=validation_steps,
        ai_coding_prompt=ai_coding_prompt,
    )


def _build_ticket_title(*, findings: list[AuditFinding]) -> str:
    lead = findings[0] if findings else None
    if lead is None:
        return "FIN-AI Code und Dokumentation angleichen"
    action_lanes = _collect_action_lanes(findings=findings)
    if action_lanes == {"jira_artifact"}:
        return f"{lead.title} in FIN-AI Artefakten konsistent aufloesen"
    if action_lanes == {"confluence_doc"}:
        return f"{lead.title} in FIN-AI Dokumentation konsistent aufloesen"
    if len(findings) == 1:
        return f"{lead.title} in FIN-AI konsistent umsetzen"
    return f"{lead.title} und angrenzende Drift in FIN-AI konsistent aufloesen"


def _build_problem_description(*, findings: list[AuditFinding]) -> str:
    fragments = [
        f"{finding.title}: {finding.summary}"
        for finding in findings
    ]
    return " ".join(fragments) if fragments else "Der Auditor hat eine relevante Soll/Ist-Abweichung erkannt."


def _build_reason(*, findings: list[AuditFinding]) -> str:
    if not findings:
        return "Code, Doku und Metamodell muessen wieder dieselbe fachliche Wahrheit transportieren."
    categories = ", ".join(sorted({finding.category for finding in findings}))
    return (
        "Die Abweichung ist fachlich relevant, weil sie Spezifikation, Implementierung und Dokumentation "
        f"auseinanderlaufen laesst. Betroffene Bewertungskategorien: {categories}."
    )


def _build_correction_measures(*, findings: list[AuditFinding]) -> list[str]:
    measures = [finding.recommendation for finding in findings]
    measures.extend(
        [
            proposed_action
            for proposed_action in (str(finding.proposed_jira_action or "").strip() for finding in findings)
            if proposed_action
        ]
    )
    write_context = _collect_write_path_details(findings=findings)
    write_deciders, write_apis, persistence_targets, sink_kinds = _write_context_tuple(write_context=write_context)
    measures.extend(
        [
            "Betroffene Codepfade auf den kanonischen Read-/Write-Vertrag ausrichten.",
            "Direkt betroffene Dokumentation und Implementierung im selben Aenderungsscope konsistent halten.",
            "Tests oder andere pruefbare Nachweise ergaenzen, damit die Korrektur stabil verifiziert werden kann.",
        ]
    )
    if write_deciders or write_apis or persistence_targets:
        sink_scope = f" zu {_format_sink_with_kind(persistence_targets[0], sink_kinds[0] if sink_kinds else '')}" if persistence_targets else ""
        measures.append(
            f"Write-Decider, DB-Write-API und Sink-Kette{sink_scope} gemeinsam angleichen statt nur den vorgeschalteten Vertrag zu aendern."
        )
    if write_context["persistence_operation_types"] or write_context["persistence_schema_targets"]:
        measures.append("Persistenz-Operation, Schema-Ziel und Adapterkette gemeinsam auf denselben Zielzustand normalisieren.")
    if write_context["schema_unconfirmed_targets"]:
        measures.append("Nicht SSOT-bestaetigte Schema-Ziele muessen explizit entschieden werden, statt aus der Testdatenbank implizit uebernommen zu werden.")
    action_lanes = _collect_action_lanes(findings=findings)
    if "jira_artifact" in action_lanes:
        measures.append("Nicht direkt im Repo korrigierbare Artefakte wie PUML, Dumps oder Metamodell-Artefakte muessen ueber ein explizites Jira-Artefakt-Ticket nachgezogen werden.")
    if "confluence_doc" in action_lanes or "confluence_and_jira" in action_lanes:
        measures.append("Confluence-SSOT muss im selben Arbeitsgang wie Code- oder Artefaktkorrekturen nachgezogen werden, damit keine neue Dokumentationsdrift entsteht.")
    return _dedupe_preserve_order(measures)


def _build_target_state(*, findings: list[AuditFinding]) -> list[str]:
    write_context = _collect_write_path_details(findings=findings)
    write_deciders, write_apis, persistence_targets, sink_kinds = _write_context_tuple(write_context=write_context)
    target = [
        "Code, Metamodell-Bezug und Dokumentation beschreiben denselben fachlichen Vertrag.",
        "Read-/Write-Verhalten der betroffenen Objekte ist eindeutig, testbar und nachvollziehbar.",
        "Offene Annahmen fuer die betroffenen Claims sind explizit aufgeloest oder sauber dokumentiert.",
    ]
    if write_deciders or write_apis or persistence_targets:
        target.append(
            "Der echte Write-Pfad ist vom fachlichen Entscheider ueber die DB-Write-API bis zum Persistenz-Sink widerspruchsfrei modelliert und umgesetzt."
        )
    if persistence_targets:
        target.append(
            f"Der betroffene Persistenz-Sink {_format_sink_with_kind(persistence_targets[0], sink_kinds[0] if sink_kinds else '')} wird konsistent und guardrail-konform beschrieben."
        )
    if write_context["persistence_backends"] or write_context["persistence_operation_types"] or write_context["persistence_schema_targets"]:
        target.append(
            "Persistenz-Backend, konkrete Write-Operation und Customer-DB-Schema-Ziel sind explizit benannt und stimmen mit Code und Doku ueberein."
        )
    if write_context["schema_validation_statuses"]:
        target.append("Schema-Ziele sind explizit als SSOT-bestaetigt, nur beobachtet oder unbestaetigt klassifiziert.")
    if any(finding.category in {"missing_definition", "clarification_needed"} for finding in findings):
        target.append("Fehlende fachliche Definitionen sind als belastbare SSOT-Aussagen nachgezogen.")
    return _dedupe_preserve_order(target)


def _build_acceptance_criteria(*, findings: list[AuditFinding], run: AuditRun) -> list[str]:
    write_context = _collect_write_path_details(findings=findings)
    write_deciders, write_apis, persistence_targets, sink_kinds = _write_context_tuple(write_context=write_context)
    criteria = [
        "Alle im Ticket beschriebenen Abweichungen sind im Code oder in der zugehoerigen Dokumentation nachvollziehbar beseitigt.",
        "Es existiert mindestens ein pruefbarer Nachweis fuer das korrigierte Verhalten, z. B. Test, deterministische Validierung oder klarer Integrationsbeleg.",
        "Der geaenderte Stand widerspricht den referenzierten Confluence- und Metamodell-Aussagen nicht mehr.",
        f"Die Umsetzung ist auf den vorgesehenen Zielstand im Repo {run.target.local_repo_path or run.target.github_repo_url or 'FIN-AI'} bezogen.",
    ]
    if write_deciders:
        criteria.append(f"Der Write-Decider {write_deciders[0]} ist fachlich und technisch auf den Zielvertrag ausgerichtet.")
    if write_apis:
        criteria.append(f"Der konkrete DB-Write-Aufruf {write_apis[0]} ist auf dem Zielpfad korrekt eingebunden.")
    if persistence_targets:
        criteria.append(
            f"Der Persistenz-Sink {_format_sink_with_kind(persistence_targets[0], sink_kinds[0] if sink_kinds else '')} wird durch denselben Zielvertrag abgesichert."
        )
    if write_context["persistence_operation_types"]:
        criteria.append(f"Die Persistenz-Operation {write_context['persistence_operation_types'][0]} bleibt nach der Aenderung fachlich korrekt.")
    if write_context["persistence_schema_targets"]:
        criteria.append(f"Das Customer-DB-Schema-Ziel {write_context['persistence_schema_targets'][0]} ist im korrigierten Write-Pfad eindeutig.")
    if write_context["schema_validation_statuses"]:
        criteria.append(f"Der Schema-Validierungsstatus {write_context['schema_validation_statuses'][0]} ist nach der Umsetzung nachvollziehbar und gewollt.")
    for finding in findings:
        if finding.canonical_key:
            criteria.append(f"Der Claim {finding.canonical_key} ist nach der Umsetzung konsistent aufgeloest.")
    return _dedupe_preserve_order(criteria)


def _build_implications(*, findings: list[AuditFinding]) -> list[str]:
    write_context = _collect_write_path_details(findings=findings)
    _write_deciders, write_apis, persistence_targets, sink_kinds = _write_context_tuple(write_context=write_context)
    implications = [
        "Nachgelagerte Entscheidungspakete und Empfehlungen muessen nach der Codeaenderung neu bewertet werden.",
        "Benachbarte Dokumentationsabschnitte oder Prozessbeschreibungen koennen von derselben Wahrheit betroffen sein.",
    ]
    if any(finding.category == "implementation_drift" for finding in findings):
        implications.append("Bestehende Runtime-Pfade und Persistenzvertraege koennen durch die Korrektur funktional beeinflusst werden.")
    if write_apis or persistence_targets:
        sink_label = _format_sink_with_kind(persistence_targets[0], sink_kinds[0] if sink_kinds else "") if persistence_targets else "den betroffenen DB-Sink"
        implications.append(
            f"Repository-/Driver-Aufrufe und der Sink-Pfad bis {sink_label} muessen nach der Aenderung gemeinsam geprueft werden."
        )
    if write_context["transaction_boundaries"] or write_context["retry_paths"] or write_context["batch_paths"]:
        implications.append("Transaktionsgrenzen sowie Retry-/Batch-Pfade koennen dieselbe Korrektur technisch verstaerken oder brechen.")
    if write_context["schema_unconfirmed_targets"]:
        implications.append("Unbestaetigte Schema-Ziele duerfen die fehlerhafte Struktur der Testdatenbank nicht stillschweigend legitimieren.")
    if any(finding.category in {"missing_definition", "open_decision"} for finding in findings):
        implications.append("Offene fachliche Entscheidungen duerfen nicht stillschweigend im Code versteckt werden.")
    return _dedupe_preserve_order(implications)


def _build_affected_parts(*, findings: list[AuditFinding]) -> list[str]:
    parts: list[str] = []
    for finding in findings:
        if finding.canonical_key:
            parts.append(f"Claim: {finding.canonical_key}")
        atomic_fact = str(finding.metadata.get("atomic_fact_summary") or "").strip()
        if atomic_fact:
            parts.append(f"Atomarer Fakt: {atomic_fact}")
        action_lane = str(finding.metadata.get("action_lane") or "").strip()
        if action_lane:
            parts.append(f"Aktionsspur: {action_lane}")
        for decider in _string_list_from_metadata(finding.metadata.get("causal_write_decider_labels"))[:2]:
            parts.append(f"Write-Decider: {decider}")
        for adapter in _string_list_from_metadata(finding.metadata.get("causal_repository_adapters"))[:2]:
            parts.append(f"Repository-Adapter: {adapter}")
        for adapter in _string_list_from_metadata(finding.metadata.get("causal_repository_adapter_symbols"))[:2]:
            parts.append(f"Repository-Symbol: {adapter}")
        for adapter in _string_list_from_metadata(finding.metadata.get("causal_driver_adapters"))[:2]:
            parts.append(f"Driver-Adapter: {adapter}")
        for adapter in _string_list_from_metadata(finding.metadata.get("causal_driver_adapter_symbols"))[:2]:
            parts.append(f"Driver-Symbol: {adapter}")
        for boundary in _string_list_from_metadata(finding.metadata.get("causal_transaction_boundaries"))[:2]:
            parts.append(f"Transaktion: {boundary}")
        for retry_path in _string_list_from_metadata(finding.metadata.get("causal_retry_paths"))[:1]:
            parts.append(f"Retry-Pfad: {retry_path}")
        for batch_path in _string_list_from_metadata(finding.metadata.get("causal_batch_paths"))[:1]:
            parts.append(f"Batch-Pfad: {batch_path}")
        for api in _string_list_from_metadata(finding.metadata.get("causal_write_apis"))[:2]:
            parts.append(f"DB-Write-API: {api}")
        sink_kinds = _string_list_from_metadata(finding.metadata.get("causal_persistence_sink_kinds"))
        for index, sink in enumerate(_string_list_from_metadata(finding.metadata.get("causal_persistence_targets"))[:2]):
            sink_kind = sink_kinds[index] if index < len(sink_kinds) else ""
            parts.append(f"Sink: {_format_sink_with_kind(sink, sink_kind)}")
        for backend in _string_list_from_metadata(finding.metadata.get("causal_persistence_backends"))[:1]:
            parts.append(f"Persistenz-Backend: {backend}")
        for operation_type in _string_list_from_metadata(finding.metadata.get("causal_persistence_operation_types"))[:2]:
            parts.append(f"Persistenz-Op: {operation_type}")
        for schema_target in _string_list_from_metadata(finding.metadata.get("causal_persistence_schema_targets"))[:2]:
            parts.append(f"Schema-Ziel: {schema_target}")
        for status in _string_list_from_metadata(finding.metadata.get("causal_schema_validation_statuses"))[:1]:
            parts.append(f"Schema-Status: {status}")
        for schema_target in _string_list_from_metadata(finding.metadata.get("causal_schema_validated_targets"))[:1]:
            parts.append(f"SSOT-bestaetigt: {schema_target}")
        for schema_target in _string_list_from_metadata(finding.metadata.get("causal_schema_observed_only_targets"))[:1]:
            parts.append(f"Nur beobachtet: {schema_target}")
        for schema_target in _string_list_from_metadata(finding.metadata.get("causal_schema_unconfirmed_targets"))[:1]:
            parts.append(f"Nicht bestaetigt: {schema_target}")
        for anchor in _string_list_from_metadata(finding.metadata.get("retrieval_anchor_values"))[:3]:
            parts.append(f"Retrieval-Anker: {anchor}")
        for location in finding.locations:
            parts.append(_location_descriptor(location=location))
    return _dedupe_preserve_order(parts) or ["Betroffene FIN-AI Services, Router, Dokumentationsanker und Metamodell-Bezuege."]


def _build_evidence(*, findings: list[AuditFinding]) -> list[str]:
    evidence: list[str] = []
    for finding in findings:
        atomic_fact = str(finding.metadata.get("atomic_fact_summary") or "").strip()
        if atomic_fact:
            evidence.append(f"Atomarer Fakt: {atomic_fact}")
        evidence.append(f"{finding.severity.upper()} {finding.title}: {finding.summary}")
        for decider in _string_list_from_metadata(finding.metadata.get("causal_write_decider_labels"))[:1]:
            evidence.append(f"Write-Decider: {decider}")
        for api in _string_list_from_metadata(finding.metadata.get("causal_write_apis"))[:1]:
            evidence.append(f"DB-Write-API: {api}")
        for adapter in _string_list_from_metadata(finding.metadata.get("causal_repository_adapter_symbols"))[:1]:
            evidence.append(f"Repository-Symbol: {adapter}")
        for adapter in _string_list_from_metadata(finding.metadata.get("causal_driver_adapter_symbols"))[:1]:
            evidence.append(f"Driver-Symbol: {adapter}")
        sink_kinds = _string_list_from_metadata(finding.metadata.get("causal_persistence_sink_kinds"))
        for index, sink in enumerate(_string_list_from_metadata(finding.metadata.get("causal_persistence_targets"))[:1]):
            sink_kind = sink_kinds[index] if index < len(sink_kinds) else ""
            evidence.append(f"Persistenz-Sink: {_format_sink_with_kind(sink, sink_kind)}")
        for backend in _string_list_from_metadata(finding.metadata.get("causal_persistence_backends"))[:1]:
            evidence.append(f"Persistenz-Backend: {backend}")
        for operation_type in _string_list_from_metadata(finding.metadata.get("causal_persistence_operation_types"))[:1]:
            evidence.append(f"Persistenz-Op: {operation_type}")
        for schema_target in _string_list_from_metadata(finding.metadata.get("causal_persistence_schema_targets"))[:1]:
            evidence.append(f"Schema-Ziel: {schema_target}")
        for status in _string_list_from_metadata(finding.metadata.get("causal_schema_validation_statuses"))[:1]:
            evidence.append(f"Schema-Status: {status}")
        for delta in _string_list_from_metadata(finding.metadata.get("delta_summary"))[:2]:
            evidence.append(f"Delta: {delta}")
        for snippet in _string_list_from_metadata(finding.metadata.get("retrieval_context"))[:2]:
            evidence.append(f"Retrieval: {snippet}")
        for location in finding.locations[:3]:
            evidence.append(_location_descriptor(location=location))
    return _dedupe_preserve_order(evidence)


def _build_implementation_notes(*, run: AuditRun, findings: list[AuditFinding]) -> list[str]:
    repo_hint = run.target.local_repo_path or run.target.github_repo_url or "FIN-AI"
    write_context = _collect_write_path_details(findings=findings)
    write_deciders, write_apis, persistence_targets, sink_kinds = _write_context_tuple(write_context=write_context)
    notes = [
        f"Arbeite im FIN-AI Zielkontext {repo_hint} auf Ref {run.target.github_ref}.",
        "Loese die Root Cause und nicht nur die sichtbaren Symptome.",
        "Halte Code und Doku konsistent; wenn ein fachlicher Vertrag geaendert wird, muss die zugehoerige Doku mitziehen.",
        "Beachte bestehende FIN-AI Guardrails, insbesondere konsistente Read-/Write-Vertraege, strikte Typisierung und sauberes Error Handling.",
        "Fuehre keine fachfremden Refactorings durch, wenn sie fuer die eigentliche Korrektur nicht notwendig sind.",
    ]
    if write_deciders:
        notes.append(f"Analysiere explizit den Write-Decider {write_deciders[0]} statt nur angrenzende Vertrags-Claims.")
    if write_apis:
        notes.append(f"Passe den konkreten DB-Write-Aufruf {write_apis[0]} oder dessen Einbindung kontrolliert an.")
    if write_context["repository_adapter_symbols"]:
        notes.append(f"Nutze fuer Repository-Aenderungen den qualifizierten Symbolpfad {write_context['repository_adapter_symbols'][0]} als Referenz.")
    if write_context["driver_adapter_symbols"]:
        notes.append(f"Nutze fuer Driver-Aenderungen den qualifizierten Symbolpfad {write_context['driver_adapter_symbols'][0]} als Referenz.")
    for proposed_action in _dedupe_preserve_order(
        [str(finding.proposed_jira_action or "").strip() for finding in findings if str(finding.proposed_jira_action or "").strip()]
    )[:2]:
        notes.append(proposed_action)
    if persistence_targets:
        notes.append(
            f"Validiere die Sink-Kette bis {_format_sink_with_kind(persistence_targets[0], sink_kinds[0] if sink_kinds else '')} und nicht nur bis zum Service-Layer."
        )
    if write_context["transaction_boundaries"]:
        notes.append(f"Beruecksichtige die Transaktionsgrenze {write_context['transaction_boundaries'][0]} in der Korrektur explizit.")
    if write_context["persistence_operation_types"] or write_context["persistence_schema_targets"]:
        notes.append("Aendere Persistenz-Operation und Customer-DB-Schema-Ziel nur kontrolliert und mit explizitem Nachweis.")
    if write_context["schema_unconfirmed_targets"]:
        notes.append("Nicht bestaetigte Schema-Ziele duerfen nicht als impliziter Zielzustand in den Code einfliessen.")
    if any(_string_list_from_metadata(finding.metadata.get("delta_statuses")) for finding in findings):
        notes.append("Beziehe die als geaendert oder neu markierten Retrieval-Segmente explizit in die Root-Cause-Analyse ein.")
    action_lanes = _collect_action_lanes(findings=findings)
    if action_lanes:
        notes.append(f"Aktionsspuren: {', '.join(sorted(action_lanes))}")
    return notes


def _build_validation_steps(*, findings: list[AuditFinding]) -> list[str]:
    write_context = _collect_write_path_details(findings=findings)
    write_deciders, write_apis, persistence_targets, sink_kinds = _write_context_tuple(write_context=write_context)
    steps = [
        "Relevante Unit-, API- oder Integrations-Tests fuer die geaenderten Pfade ausfuehren oder gezielt ergaenzen.",
        "Pruefen, ob die referenzierten Doku- und Prozessaussagen nach der Aenderung noch zutreffen.",
        "Sicherstellen, dass keine neue Soll/Ist-Drift fuer angrenzende Objekte eingefuehrt wurde.",
    ]
    if any(finding.category == "implementation_drift" for finding in findings):
        steps.append("Den konkret betroffenen Read-/Write-Pfad end-to-end gegen den erwarteten Vertrag pruefen.")
    if write_deciders or write_apis or persistence_targets:
        sink_label = _format_sink_with_kind(persistence_targets[0], sink_kinds[0] if sink_kinds else "") if persistence_targets else "den betroffenen DB-Sink"
        path_bits = [item for item in [write_deciders[0] if write_deciders else "", write_apis[0] if write_apis else "", sink_label] if item]
        steps.append(f"Den echten Write-Pfad {' -> '.join(path_bits)} end-to-end gegen den Zielzustand pruefen.")
    if write_context["transaction_boundaries"] or write_context["retry_paths"] or write_context["batch_paths"]:
        steps.append("Transaktions-, Retry- und Batch-Verhalten der korrigierten Schreibpfade explizit verifizieren.")
    if write_context["persistence_operation_types"] or write_context["persistence_schema_targets"]:
        steps.append("Persistenz-Operationstyp und Schema-Ziel gegen die erwartete Customer-DB-Struktur pruefen.")
    if write_context["schema_validation_statuses"]:
        steps.append("SSOT-bestaetigte, nur beobachtete und unbestaetigte Schema-Ziele getrennt validieren.")
    return _dedupe_preserve_order(steps)


def _build_ai_coding_prompt(
    *,
    run: AuditRun,
    title: str,
    problem_description: str,
    reason: str,
    correction_measures: list[str],
    target_state: list[str],
    acceptance_criteria: list[str],
    implications: list[str],
    affected_parts: list[str],
    evidence: list[str],
    implementation_notes: list[str],
    validation_steps: list[str],
) -> str:
    repo_hint = run.target.local_repo_path or run.target.github_repo_url or "FIN-AI"
    return "\n".join(
        [
            f"Arbeite im FIN-AI Repo-Kontext {repo_hint} auf Ref {run.target.github_ref}.",
            f"Aufgabe: {title}",
            f"Problem: {problem_description}",
            f"Grund: {reason}",
            "Korrekturmassnahmen:",
            *[f"- {item}" for item in correction_measures],
            "Erwartetes Zielbild:",
            *[f"- {item}" for item in target_state],
            "Pruefbare Abnahmekriterien:",
            *[f"- {item}" for item in acceptance_criteria],
            "Implikationen:",
            *[f"- {item}" for item in implications],
            "Betroffene Teile:",
            *[f"- {item}" for item in affected_parts],
            "Evidenz:",
            *[f"- {item}" for item in evidence],
            "Implementierungshinweise:",
            *[f"- {item}" for item in implementation_notes],
            "Validierung:",
            *[f"- {item}" for item in validation_steps],
            "Wichtig: Halte Doku und Code konsistent, fuehre keine Safety- oder Guardrail-Lockerung ein und liefere die Korrektur so, dass der Auditor die Abweichung danach nicht erneut meldet.",
        ]
    )


def _location_descriptor(*, location: AuditLocation) -> str:
    anchor = location.position.anchor_value if location.position else None
    fragments = [location.title]
    if location.path_hint:
        fragments.append(location.path_hint)
    if anchor:
        fragments.append(anchor)
    if location.url:
        fragments.append(location.url)
    return " | ".join(fragment for fragment in fragments if fragment)


def _collect_action_lanes(*, findings: list[AuditFinding]) -> set[str]:
    lanes: set[str] = set()
    for finding in findings:
        lane = str(finding.metadata.get("action_lane") or "").strip()
        if lane:
            lanes.add(lane)
    return lanes


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _string_list_from_metadata(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _collect_write_path_context(*, findings: list[AuditFinding]) -> tuple[list[str], list[str], list[str], list[str]]:
    return _write_context_tuple(write_context=_collect_write_path_details(findings=findings))


def _collect_write_path_details(*, findings: list[AuditFinding]) -> dict[str, list[str]]:
    return {
        "write_deciders": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_write_decider_labels"))
            ]
        ),
        "write_apis": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_write_apis"))
            ]
        ),
        "repository_adapters": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_repository_adapters"))
            ]
        ),
        "repository_adapter_symbols": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_repository_adapter_symbols"))
            ]
        ),
        "driver_adapters": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_driver_adapters"))
            ]
        ),
        "driver_adapter_symbols": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_driver_adapter_symbols"))
            ]
        ),
        "transaction_boundaries": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_transaction_boundaries"))
            ]
        ),
        "retry_paths": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_retry_paths"))
            ]
        ),
        "batch_paths": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_batch_paths"))
            ]
        ),
        "persistence_targets": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_persistence_targets"))
            ]
        ),
        "sink_kinds": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_persistence_sink_kinds"))
            ]
        ),
        "persistence_backends": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_persistence_backends"))
            ]
        ),
        "persistence_operation_types": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_persistence_operation_types"))
            ]
        ),
        "persistence_schema_targets": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_persistence_schema_targets"))
            ]
        ),
        "schema_validated_targets": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_schema_validated_targets"))
            ]
        ),
        "schema_observed_only_targets": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_schema_observed_only_targets"))
            ]
        ),
        "schema_unconfirmed_targets": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_schema_unconfirmed_targets"))
            ]
        ),
        "schema_validation_statuses": _dedupe_preserve_order(
            [
                item
                for finding in findings
                for item in _string_list_from_metadata(finding.metadata.get("causal_schema_validation_statuses"))
            ]
        ),
    }


def _write_context_tuple(*, write_context: dict[str, list[str]]) -> tuple[list[str], list[str], list[str], list[str]]:
    return (
        write_context["write_deciders"],
        write_context["write_apis"],
        write_context["persistence_targets"],
        write_context["sink_kinds"],
    )


def _format_sink_with_kind(sink: str, sink_kind: str) -> str:
    normalized_sink = str(sink or "").strip()
    if not normalized_sink:
        return ""
    kind_label = {
        "node_sink": "Node-Sink",
        "relationship_sink": "Relationship-Sink",
        "history_sink": "History-Sink",
    }.get(str(sink_kind or "").strip(), "")
    return f"{kind_label} -> {normalized_sink}" if kind_label else normalized_sink
