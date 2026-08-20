package proxycontrol

import (
	"cmp"
	"slices"
	"strings"
)

type candidate struct {
	ID                  int
	Protocol            string
	Tags                []string
	AverageResponseTime int
	YouTubeResponseTime *int
	YouTubeFailureScore float64
	NetworkIdentityKey  string
	CountryPreference   int64
	TagPreference       int64
	IdentityPreference  int64
	ValidityPreference  int64
}

type slotState struct {
	Name    string
	Role    string
	Number  int
	ProxyID *int
	Locked  bool
}

type assignmentPlan struct {
	BySlot  map[string]*int
	Reserve []int
}

func planPolicyAssignments(
	slots []slotState,
	candidatesByRole map[string][]candidate,
) assignmentPlan {
	orderedSlots := append([]slotState(nil), slots...)
	slices.SortFunc(orderedSlots, compareSlots)
	result := assignmentPlan{BySlot: make(map[string]*int, len(orderedSlots))}
	used := make(map[int]bool)

	for _, slot := range orderedSlots {
		if !slot.Locked || slot.ProxyID == nil || used[*slot.ProxyID] {
			continue
		}
		proxyID := *slot.ProxyID
		result.BySlot[slot.Name] = &proxyID
		used[proxyID] = true
	}

	for _, role := range []string{RoleDiscover, RoleChannel, RoleQueryQuality, RoleDetail} {
		roleSlots := make([]slotState, 0)
		for _, slot := range orderedSlots {
			if slot.Role == role && !slot.Locked {
				roleSlots = append(roleSlots, slot)
			}
		}
		available := make([]candidate, 0, len(candidatesByRole[role]))
		for _, item := range candidatesByRole[role] {
			if !used[item.ID] {
				available = append(available, item)
			}
		}
		rolePlan := planAssignments(roleSlots, available)
		for slotName, proxyID := range rolePlan.BySlot {
			result.BySlot[slotName] = proxyID
			if proxyID != nil {
				used[*proxyID] = true
			}
		}
	}

	reserveSeen := make(map[int]bool)
	for _, role := range []string{RoleDiscover, RoleChannel, RoleQueryQuality, RoleDetail} {
		for _, item := range candidatesByRole[role] {
			if used[item.ID] || reserveSeen[item.ID] {
				continue
			}
			reserveSeen[item.ID] = true
			result.Reserve = append(result.Reserve, item.ID)
		}
	}
	slices.Sort(result.Reserve)
	for _, slot := range orderedSlots {
		if _, found := result.BySlot[slot.Name]; !found {
			result.BySlot[slot.Name] = nil
		}
	}
	return result
}

func planAssignments(slots []slotState, candidates []candidate) assignmentPlan {
	orderedSlots := append([]slotState(nil), slots...)
	slices.SortFunc(orderedSlots, compareSlots)
	orderedCandidates := append([]candidate(nil), candidates...)
	slices.SortFunc(orderedCandidates, compareCandidates)

	availableRoles := make(map[string]bool, 4)
	byID := make(map[int]candidate, len(orderedCandidates))
	for _, slot := range orderedSlots {
		availableRoles[slot.Role] = true
	}
	for _, item := range orderedCandidates {
		byID[item.ID] = item
	}

	plan := assignmentPlan{BySlot: make(map[string]*int, len(orderedSlots))}
	used := make(map[int]bool, len(orderedCandidates))
	assign := func(slot string, proxyID int) {
		value := proxyID
		plan.BySlot[slot] = &value
		used[proxyID] = true
	}

	// A leased slot retains its exact endpoint even when the endpoint has just
	// entered cooldown or failure observation. The current task may fail, but it
	// must never continue from a different IP.
	for _, slot := range orderedSlots {
		if !slot.Locked || slot.ProxyID == nil || used[*slot.ProxyID] {
			continue
		}
		assign(slot.Name, *slot.ProxyID)
	}

	// Preserve already-correct role-pinned assignments first.
	for _, slot := range orderedSlots {
		if _, assigned := plan.BySlot[slot.Name]; assigned || slot.ProxyID == nil {
			continue
		}
		item, eligible := byID[*slot.ProxyID]
		if !eligible || used[item.ID] || pinnedRole(item, availableRoles) != slot.Role {
			continue
		}
		assign(slot.Name, item.ID)
	}

	// Then place remaining pinned endpoints in their intended role.
	for _, item := range orderedCandidates {
		role := pinnedRole(item, availableRoles)
		if role == "" || used[item.ID] {
			continue
		}
		for _, slot := range orderedSlots {
			if slot.Role == role {
				if _, occupied := plan.BySlot[slot.Name]; !occupied {
					assign(slot.Name, item.ID)
					break
				}
			}
		}
	}

	// Keep healthy generic assignments stable before filling empty slots.
	for _, slot := range orderedSlots {
		if _, assigned := plan.BySlot[slot.Name]; assigned || slot.ProxyID == nil {
			continue
		}
		item, eligible := byID[*slot.ProxyID]
		if !eligible || used[item.ID] || pinnedRole(item, availableRoles) != "" {
			continue
		}
		assign(slot.Name, item.ID)
	}

	for _, slot := range orderedSlots {
		if _, assigned := plan.BySlot[slot.Name]; assigned {
			continue
		}
		var selected *candidate
		for index := range orderedCandidates {
			item := &orderedCandidates[index]
			if !used[item.ID] && pinnedRole(*item, availableRoles) == "" {
				selected = item
				break
			}
		}
		if selected == nil {
			plan.BySlot[slot.Name] = nil
			continue
		}
		assign(slot.Name, selected.ID)
	}

	for _, item := range orderedCandidates {
		if !used[item.ID] {
			plan.Reserve = append(plan.Reserve, item.ID)
		}
	}
	return plan
}

func compareSlots(left, right slotState) int {
	if priority := cmp.Compare(rolePriority(left.Role), rolePriority(right.Role)); priority != 0 {
		return priority
	}
	if number := cmp.Compare(left.Number, right.Number); number != 0 {
		return number
	}
	return cmp.Compare(left.Name, right.Name)
}

func compareCandidates(left, right candidate) int {
	leftPriority := candidatePriority(left)
	rightPriority := candidatePriority(right)
	for index := range leftPriority {
		if result := cmp.Compare(leftPriority[index], rightPriority[index]); result != 0 {
			return result
		}
	}
	return 0
}

func candidatePriority(item candidate) []int64 {
	residential := int64(1)
	for _, tag := range item.Tags {
		if strings.Contains(strings.ToLower(tag), "residential") {
			residential = 0
			break
		}
	}
	protocol := int64(1)
	if strings.HasPrefix(strings.ToLower(item.Protocol), "http") {
		protocol = 0
	}
	timingMissing := int64(1)
	responseTime := int64(item.AverageResponseTime)
	if item.YouTubeResponseTime != nil && *item.YouTubeResponseTime > 0 {
		timingMissing = 0
		responseTime = int64(*item.YouTubeResponseTime)
	}
	if responseTime <= 0 {
		responseTime = int64(^uint(0) >> 1)
	}
	failureScore := item.YouTubeFailureScore
	if failureScore < 0 {
		failureScore = 0
	}
	return []int64{
		item.CountryPreference,
		item.TagPreference,
		item.IdentityPreference,
		item.ValidityPreference,
		residential,
		protocol,
		int64(failureScore * 1_000_000),
		timingMissing,
		responseTime,
		int64(item.ID),
	}
}

func pinnedRole(item candidate, available map[string]bool) string {
	for _, role := range []string{RoleDiscover, RoleChannel, RoleQueryQuality, RoleDetail} {
		for _, tag := range item.Tags {
			normalized := strings.ToLower(strings.TrimSpace(tag))
			if normalized != "bullmq-"+role && normalized != "role:"+role {
				continue
			}
			if available[role] {
				return role
			}
			if role == RoleDetail && available[RoleChannel] {
				return RoleChannel
			}
			return ""
		}
	}
	return ""
}

func rolePriority(role string) int {
	switch role {
	case RoleDiscover:
		return 0
	case RoleChannel:
		return 1
	case RoleQueryQuality:
		return 2
	case RoleDetail:
		return 3
	default:
		return 99
	}
}

func minimumReserveWatermark(workerCount, percent, minimum int) int {
	if workerCount < 0 {
		workerCount = 0
	}
	if percent < 0 {
		percent = 0
	}
	requested := (workerCount*percent + 99) / 100
	if requested < minimum {
		return minimum
	}
	return requested
}
