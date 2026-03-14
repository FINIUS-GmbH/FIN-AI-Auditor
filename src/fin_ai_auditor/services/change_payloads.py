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
            "Betroffene Codepfade auf den kanonischen Read-/Write-Vertrag ausrichten.",
            "Direkt betroffene Dokumentation und Implementierung im selben Aenderungsscope konsistent halten.",
            "Tests oder andere pruefbare Nachweise ergaenzen, damit die Korrektur stabil verifiziert werden kann.",
        ]
    )
    return _dedupe_preserve_order(measures)


def _build_target_state(*, findings: list[AuditFinding]) -> list[str]:
    target = [
        "Code, Metamodell-Bezug und Dokumentation beschreiben denselben fachlichen Vertrag.",
        "Read-/Write-Verhalten der betroffenen Objekte ist eindeutig, testbar und nachvollziehbar.",
        "Offene Annahmen fuer die betroffenen Claims sind explizit aufgeloest oder sauber dokumentiert.",
    ]
    if any(finding.category in {"missing_definition", "clarification_needed"} for finding in findings):
        target.append("Fehlende fachliche Definitionen sind als belastbare SSOT-Aussagen nachgezogen.")
    return _dedupe_preserve_order(target)


def _build_acceptance_criteria(*, findings: list[AuditFinding], run: AuditRun) -> list[str]:
    criteria = [
        "Alle im Ticket beschriebenen Abweichungen sind im Code oder in der zugehoerigen Dokumentation nachvollziehbar beseitigt.",
        "Es existiert mindestens ein pruefbarer Nachweis fuer das korrigierte Verhalten, z. B. Test, deterministische Validierung oder klarer Integrationsbeleg.",
        "Der geaenderte Stand widerspricht den referenzierten Confluence- und Metamodell-Aussagen nicht mehr.",
        f"Die Umsetzung ist auf den vorgesehenen Zielstand im Repo {run.target.local_repo_path or run.target.github_repo_url or 'FIN-AI'} bezogen.",
    ]
    for finding in findings:
        if finding.canonical_key:
            criteria.append(f"Der Claim {finding.canonical_key} ist nach der Umsetzung konsistent aufgeloest.")
    return _dedupe_preserve_order(criteria)


def _build_implications(*, findings: list[AuditFinding]) -> list[str]:
    implications = [
        "Nachgelagerte Entscheidungspakete und Empfehlungen muessen nach der Codeaenderung neu bewertet werden.",
        "Benachbarte Dokumentationsabschnitte oder Prozessbeschreibungen koennen von derselben Wahrheit betroffen sein.",
    ]
    if any(finding.category == "implementation_drift" for finding in findings):
        implications.append("Bestehende Runtime-Pfade und Persistenzvertraege koennen durch die Korrektur funktional beeinflusst werden.")
    if any(finding.category in {"missing_definition", "open_decision"} for finding in findings):
        implications.append("Offene fachliche Entscheidungen duerfen nicht stillschweigend im Code versteckt werden.")
    return _dedupe_preserve_order(implications)


def _build_affected_parts(*, findings: list[AuditFinding]) -> list[str]:
    parts: list[str] = []
    for finding in findings:
        if finding.canonical_key:
            parts.append(f"Claim: {finding.canonical_key}")
        for anchor in _string_list_from_metadata(finding.metadata.get("retrieval_anchor_values"))[:3]:
            parts.append(f"Retrieval-Anker: {anchor}")
        for location in finding.locations:
            parts.append(_location_descriptor(location=location))
    return _dedupe_preserve_order(parts) or ["Betroffene FIN-AI Services, Router, Dokumentationsanker und Metamodell-Bezuege."]


def _build_evidence(*, findings: list[AuditFinding]) -> list[str]:
    evidence: list[str] = []
    for finding in findings:
        evidence.append(f"{finding.severity.upper()} {finding.title}: {finding.summary}")
        for delta in _string_list_from_metadata(finding.metadata.get("delta_summary"))[:2]:
            evidence.append(f"Delta: {delta}")
        for snippet in _string_list_from_metadata(finding.metadata.get("retrieval_context"))[:2]:
            evidence.append(f"Retrieval: {snippet}")
        for location in finding.locations[:3]:
            evidence.append(_location_descriptor(location=location))
    return _dedupe_preserve_order(evidence)


def _build_implementation_notes(*, run: AuditRun, findings: list[AuditFinding]) -> list[str]:
    repo_hint = run.target.local_repo_path or run.target.github_repo_url or "FIN-AI"
    notes = [
        f"Arbeite im FIN-AI Zielkontext {repo_hint} auf Ref {run.target.github_ref}.",
        "Loese die Root Cause und nicht nur die sichtbaren Symptome.",
        "Halte Code und Doku konsistent; wenn ein fachlicher Vertrag geaendert wird, muss die zugehoerige Doku mitziehen.",
        "Beachte bestehende FIN-AI Guardrails, insbesondere konsistente Read-/Write-Vertraege, strikte Typisierung und sauberes Error Handling.",
        "Fuehre keine fachfremden Refactorings durch, wenn sie fuer die eigentliche Korrektur nicht notwendig sind.",
    ]
    if any(_string_list_from_metadata(finding.metadata.get("delta_statuses")) for finding in findings):
        notes.append("Beziehe die als geaendert oder neu markierten Retrieval-Segmente explizit in die Root-Cause-Analyse ein.")
    return notes


def _build_validation_steps(*, findings: list[AuditFinding]) -> list[str]:
    steps = [
        "Relevante Unit-, API- oder Integrations-Tests fuer die geaenderten Pfade ausfuehren oder gezielt ergaenzen.",
        "Pruefen, ob die referenzierten Doku- und Prozessaussagen nach der Aenderung noch zutreffen.",
        "Sicherstellen, dass keine neue Soll/Ist-Drift fuer angrenzende Objekte eingefuehrt wurde.",
    ]
    if any(finding.category == "implementation_drift" for finding in findings):
        steps.append("Den konkret betroffenen Read-/Write-Pfad end-to-end gegen den erwarteten Vertrag pruefen.")
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
