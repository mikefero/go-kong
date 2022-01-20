package kong

import (
	"context"
	"encoding/json"
	"fmt"
)

// AbstractConsumerGroupService handles ConsumerGroups in Kong.
type AbstractConsumerGroupService interface {
	// Create creates a ConsumerGroup in Kong.
	Create(ctx context.Context, consumerGroup *ConsumerGroup) (*ConsumerGroup, error)
	// Get fetches a ConsumerGroup in Kong.
	Get(ctx context.Context, nameOrID *string) (*ConsumerGroup, error)
	// Update updates a ConsumerGroup in Kong
	Update(ctx context.Context, consumerGroup *ConsumerGroup) (*ConsumerGroup, error)
	// Delete deletes a ConsumerGroup in Kong
	Delete(ctx context.Context, nameOrID *string) error
	// List fetches a list of ConsumerGroups in Kong.
	List(ctx context.Context, opt *ListOpt) ([]*ConsumerGroup, *ListOpt, error)
	// ListAll fetches all ConsumerGroups in Kong.
	ListAll(ctx context.Context) ([]*ConsumerGroup, error)
}

// ConsumerGroupService handles ConsumerGroups in Kong.
type ConsumerGroupService service

// wrappedConsumerGroup is an object that contains an actual ConsumerGroup under a consumer_group key
// The consumer group API is weird and returns additional context (consumer lists, etc.) rather than just the group
type wrappedConsumerGroup struct {
	ConsumerGroup *ConsumerGroup `json:"consumer_group,omitempty" yaml:"consumer_group,omitempty"`
}

// Create creates a ConsumerGroup in Kong.
// If an ID is specified, it will be used to
// create a consumerGroup in Kong, otherwise an ID
// is auto-generated.
func (s *ConsumerGroupService) Create(ctx context.Context,
	consumerGroup *ConsumerGroup) (*ConsumerGroup, error) {

	queryPath := "/consumer_groups"
	method := "POST"
	if consumerGroup.ID != nil {
		queryPath = queryPath + "/" + *consumerGroup.ID
		method = "PUT"
	}
	req, err := s.client.NewRequest(method, queryPath, nil, consumerGroup)
	if err != nil {
		return nil, err
	}

	var createdConsumerGroup ConsumerGroup
	_, err = s.client.Do(ctx, req, &createdConsumerGroup)
	if err != nil {
		return nil, err
	}
	return &createdConsumerGroup, nil
}

// Get fetches a ConsumerGroup in Kong.
func (s *ConsumerGroupService) Get(ctx context.Context,
	nameOrID *string) (*ConsumerGroup, error) {

	if isEmptyString(nameOrID) {
		return nil, fmt.Errorf("nameOrID cannot be nil for Get operation")
	}

	endpoint := fmt.Sprintf("/consumer_groups/%v", *nameOrID)
	req, err := s.client.NewRequest("GET", endpoint, nil, nil)
	if err != nil {
		return nil, err
	}

	var wrapped wrappedConsumerGroup
	_, err = s.client.Do(ctx, req, &wrapped)
	if err != nil {
		return nil, err
	}
	return wrapped.ConsumerGroup, nil
}

// Update updates a ConsumerGroup in Kong
func (s *ConsumerGroupService) Update(ctx context.Context,
	consumerGroup *ConsumerGroup) (*ConsumerGroup, error) {

	if isEmptyString(consumerGroup.ID) {
		return nil, fmt.Errorf("ID cannot be nil for Update operation")
	}

	endpoint := fmt.Sprintf("/consumer_groups/%v", *consumerGroup.ID)
	req, err := s.client.NewRequest("PATCH", endpoint, nil, consumerGroup)
	if err != nil {
		return nil, err
	}

	var updatedAPI ConsumerGroup
	_, err = s.client.Do(ctx, req, &updatedAPI)
	if err != nil {
		return nil, err
	}
	return &updatedAPI, nil
}

// Delete deletes a ConsumerGroup in Kong
func (s *ConsumerGroupService) Delete(ctx context.Context,
	nameOrID *string) error {

	if isEmptyString(nameOrID) {
		return fmt.Errorf("nameOrID cannot be nil for Delete operation")
	}

	endpoint := fmt.Sprintf("/consumer_groups/%v", *nameOrID)
	req, err := s.client.NewRequest("DELETE", endpoint, nil, nil)
	if err != nil {
		return err
	}

	_, err = s.client.Do(ctx, req, nil)
	return err
}

// List fetches a list of ConsumerGroups in Kong.
// opt can be used to control pagination.
func (s *ConsumerGroupService) List(ctx context.Context,
	opt *ListOpt) ([]*ConsumerGroup, *ListOpt, error) {
	data, next, err := s.client.list(ctx, "/consumer_groups", opt)
	if err != nil {
		return nil, nil, err
	}
	var consumerGroups []*ConsumerGroup

	for _, object := range data {
		b, err := object.MarshalJSON()
		if err != nil {
			return nil, nil, err
		}
		var consumerGroup ConsumerGroup
		err = json.Unmarshal(b, &consumerGroup)
		if err != nil {
			return nil, nil, err
		}
		consumerGroups = append(consumerGroups, &consumerGroup)
	}

	return consumerGroups, next, nil
}

// ListAll fetches all ConsumerGroups in Kong.
// This method can take a while if there
// a lot of ConsumerGroups present.
func (s *ConsumerGroupService) ListAll(ctx context.Context) ([]*ConsumerGroup, error) {
	var consumerGroups, data []*ConsumerGroup
	var err error
	opt := &ListOpt{Size: pageSize}

	for opt != nil {
		data, opt, err = s.List(ctx, opt)
		if err != nil {
			return nil, err
		}
		consumerGroups = append(consumerGroups, data...)
	}
	return consumerGroups, nil
}
