# Bloodraven Commercial License

**ShipStream, LLC — Commercial License Terms, Version 1.0 (2026-08-13)**

Bloodraven is distributed under the [Business Source License 1.1](./LICENSE)
(BSL). The BSL grants free Production Use to individuals, non-commercial users,
non-profits, and entities under USD $1,000,000 in annual revenue. Everyone else
needs a commercial license to run Bloodraven in production. These are the terms
of that license.

> These terms are a plain-language commercial agreement, not legal advice. If
> your organization requires a signed order form, redlines, a W-9, a vendor
> security questionnaire, or a purchase order, email
> **licensing@shipstream.io** before purchasing.

---

## 1. Definitions

**"Licensor"** means ShipStream, LLC, a limited liability company.

**"Licensee"** means the legal entity identified on the purchase receipt,
together with all entities under common control with it.

**"Software"** means Bloodraven, including the operator, the sidecar, the
`kubectl-bloodraven` plugin, the Helm charts, and the custom resource
definitions distributed with them.

**"Failover Group"** means the set of database instances and associated
resources managed under a single `MySQLFailoverGroup` custom resource, together
with any cache or session resources co-managed with it. A Failover Group counts
as one Failover Group regardless of how many sites, replicas, or read-only
instances it contains.

**"Production Use"** has the meaning given in the [BSL Additional Use
Grant](./LICENSE).

**"Update Period"** means the period during which Licensee is entitled to
receive new versions of the Software, as described in Section 4.

---

## 2. Grant of License

Subject to payment of the applicable fee and Licensee's compliance with these
terms, Licensor grants Licensee a **perpetual, worldwide, non-exclusive,
non-transferable, non-sublicensable** license to:

1. install, run, and make Production Use of the Software, up to the quantity of
   Failover Groups purchased;
2. modify the Software and run those modifications internally; and
3. make copies of the Software as reasonably necessary for backup, disaster
   recovery, and internal distribution to Licensee's personnel.

This license applies to every version of the Software published during the
Update Period. Licensee may continue to run those versions **indefinitely**,
including after the Update Period ends. The license does not expire.

---

## 3. Editions and Pricing

All prices are in United States dollars and are **one-time** charges.

| Edition | Price | Scope |
|---|---|---|
| **Community** | Free | All non-production use, forever. Plus Production Use by individuals, non-commercial users, non-profits, and entities under USD $1,000,000 annual revenue. No purchase required — this is the [BSL](./LICENSE) grant. |
| **Production** | **$990** per Failover Group | Production Use of one Failover Group. Includes 12 months of updates. |
| **Organization** | **$4,900** | Production Use of unlimited Failover Groups within one legal entity and its controlled affiliates. Includes 12 months of updates. |

Optional renewals, to extend the Update Period after the initial 12 months:

| Renewal | Price |
|---|---|
| Production, per Failover Group | **$390** per year |
| Organization | **$1,900** per year |

Renewal is optional. Licensee's right to run every version published during a
paid Update Period is perpetual and survives non-renewal (Section 2).

Optionally available, sold separately:

| Add-on | Price | What it is |
|---|---|---|
| **Priority Triage** | **$6,000** per year | Named email channel with best-effort response during US business hours, priority on issue triage, and direct input on the roadmap. **No SLA, no uptime guarantee, no on-call, and no emergency incident response.** |

Pricing is subject to change for new purchases. Changes do not affect licenses
already purchased.

---

## 4. Updates

Each purchase includes **12 months** of updates, beginning on the purchase date.
During the Update Period, Licensee is entitled to every version of the Software
that Licensor publishes, including feature releases, bug fixes, and security
patches.

When the Update Period ends:

- Licensee keeps the perpetual right to run every version published during it.
- Licensee stops receiving new versions until a renewal is purchased.
- Purchasing a renewal restores access to the current version and starts a new
  12-month Update Period.

Licensor does not guarantee any release cadence, any particular feature, or
continued maintenance of any version.

---

## 5. Counting Failover Groups

Licensee needs one Production license per Failover Group in Production Use,
counted as the **maximum number in concurrent Production Use** at any point
during the Update Period.

The following do **not** consume a license:

- Development, staging, testing, evaluation, CI, demo, and training clusters.
- Failover Groups that have been permanently decommissioned.
- Additional sites, replicas, or read-only instances within a Failover Group
  already licensed.

The Organization edition removes counting entirely and is usually the cheaper
choice at five or more Failover Groups.

---

## 6. What Is Not Included

Licensor sells software, not a service. This license does **not** include:

- Technical support, unless Priority Triage is purchased separately.
- Any service level agreement, uptime commitment, or response time commitment.
- Incident response, on-call coverage, or emergency assistance.
- Installation, configuration, migration, or consulting services.
- Custom development or guaranteed feature delivery.
- Indemnification, unless separately agreed in writing.

Documentation, runbooks, the published chaos and simulation test results, and
the course materials are provided free to everyone and are the primary support
channel. Bug reports are welcome on GitHub from licensees and non-licensees
alike and are handled on a best-effort basis.

---

## 7. Restrictions

Licensee may not:

1. redistribute, resell, sublicense, lease, or lend the Software, in original or
   modified form, to any third party;
2. offer the Software, or any substantially similar derivative of it, to third
   parties as a hosted, managed, or embedded product or service;
3. use the Software to provide a managed database service to third parties;
4. remove, obscure, or alter any copyright, license, or attribution notice in
   the Software; or
5. use Licensor's trademarks, trade names, or logos except to accurately
   identify the Software.

Restriction 1 does not prevent Licensee from operating the Software on behalf of
its own customers as part of a service that is not itself a database management
or database hosting offering — for example, running Bloodraven behind Licensee's
own SaaS application. If you are unsure which side of this line you are on,
email **licensing@shipstream.io** and ask; the answer is free.

---

## 8. License Verification

Licensor does not use license servers, activation checks, phone-home telemetry,
or feature gating. The Software is fully functional without a license key and
does not contact Licensor.

On purchase, Licensee receives a license key. Recording it in the
`MySQLFailoverGroup` spec causes the operator to log the licensee name, edition,
and Update Period expiry at startup and to expose them as Prometheus metric
labels. This exists so Licensee has an in-cluster artifact for its own
compliance and audit purposes. It is optional, verified entirely offline, and
never affects operator behavior.

Compliance is on the honor system. The purchase receipt is the license.

---

## 9. Warranty Disclaimer

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE, TITLE, AND NON-INFRINGEMENT.

Bloodraven performs automated failover of production databases. Failover is a
destructive operation under some failure conditions, and Bloodraven does not
provide synchronous replication or a zero recovery point objective. Licensee is
solely responsible for evaluating its suitability, for testing it against
Licensee's own failure modes, and for maintaining independent backups.

---

## 10. Limitation of Liability

TO THE MAXIMUM EXTENT PERMITTED BY LAW, LICENSOR'S TOTAL AGGREGATE LIABILITY
ARISING OUT OF OR RELATING TO THIS LICENSE OR THE SOFTWARE WILL NOT EXCEED THE
AMOUNT LICENSEE PAID TO LICENSOR IN THE TWELVE MONTHS PRECEDING THE EVENT GIVING
RISE TO THE CLAIM.

IN NO EVENT WILL LICENSOR BE LIABLE FOR ANY INDIRECT, INCIDENTAL, SPECIAL,
CONSEQUENTIAL, OR PUNITIVE DAMAGES, OR FOR ANY LOSS OF PROFITS, REVENUE, DATA,
OR DATA USE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.

---

## 11. Term and Termination

This license is perpetual and has no expiration date.

Licensor may terminate it only if Licensee materially breaches Section 7
(Restrictions) and fails to cure the breach within 30 days of written notice.
On termination, Licensee must stop all Production Use of the Software.

Fees are non-refundable except as stated in Section 12.

---

## 12. Refunds

Licensor offers a **30-day, no-questions-asked refund** on any license purchase.
Email **licensing@shipstream.io** with the receipt. Renewals are refundable on
the same terms.

---

## 13. Purchasing

Licenses are sold through [Polar](https://polar.sh), which acts as the merchant
of record and handles VAT, sales tax, and invoicing. Polar issues a compliant
invoice suitable for expense reimbursement and procurement records.

Purchase links are on the [licensing page](https://bloodraven.readthedocs.io/en/latest/docs/licensing).

For volume pricing, multi-year terms, a signed agreement, or purchase-order
billing, email **licensing@shipstream.io**.

---

## 14. General

**Assignment.** Licensee may assign this license to a successor in interest in
connection with a merger, acquisition, or sale of substantially all assets, on
written notice to Licensor. Licensee may not otherwise assign it.

**Governing law.** This license is governed by the laws of the State of
Tennessee, United States, without regard to its conflict of laws rules. The
exclusive venue for disputes is the state and federal courts located in
Tennessee.

**Entire agreement.** These terms, together with the purchase receipt, are the
entire agreement between the parties regarding the Software and supersede any
conflicting terms in a Licensee purchase order or vendor form, unless Licensor
has signed a separate written agreement.

**Severability.** If any provision is held unenforceable, the remaining
provisions remain in effect.

**Changes.** Licensor may revise these terms for future purchases. The version
in effect on Licensee's purchase date governs that purchase.

---

Questions: **licensing@shipstream.io**
