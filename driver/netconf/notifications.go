package netconf

import (
	"fmt"

	"github.com/scrapli/scrapligo/response"
	"github.com/scrapli/scrapligo/util"
)

// Notifications returns a read-only channel of raw RFC 5277 <notification>
// frames captured by the read loop. The channel stays open for the life
// of the Driver; consumers should drain promptly to avoid having frames
// dropped (the read loop is non-blocking on this channel and will log a
// warning when it has to discard).
//
// Typical use:
//
//	if _, err := d.CreateSubscription("NETCONF", "", "", ""); err != nil { return err }
//	for frame := range d.Notifications() {
//	    // parse + handle frame
//	}
//
// The channel does not deliver YANG-Push (RFC 8641) frames; those are
// keyed on <subscription-id> and remain accessible via
// GetSubscriptionMessages. Notifications() is for the legacy /
// default-stream interface only.
func (d *Driver) Notifications() <-chan []byte {
	return d.notifications
}

// deliverNotification routes one captured frame onto the notifications
// channel without blocking the read loop. Frames are deep-copied so the
// underlying buffer can be reused immediately.
func (d *Driver) deliverNotification(frame []byte) {
	if d.notifications == nil {
		return
	}
	cp := make([]byte, len(frame))
	copy(cp, frame)
	select {
	case d.notifications <- cp:
	default:
		// Buffer full; drop with a warn-level log. Notifications
		// are best-effort — dropping is better than back-pressuring
		// the read loop into a hung session.
		d.Logger.Debugf(
			"notifications channel full (cap=%d); dropping a frame",
			cap(d.notifications),
		)
	}
}

// closeNotifications shuts the notifications channel down. Safe to
// call multiple times; idempotent via sync.Once. Called from Driver.Close.
func (d *Driver) closeNotifications() {
	d.notificationsOnce.Do(func() {
		if d.notifications != nil {
			close(d.notifications)
		}
	})
}

// createSubscriptionElem builds the standard RFC 5277 <create-subscription>
// element. Stream defaults to "NETCONF" when empty. Filter / startTime /
// stopTime are optional and only added when set.
type createSubscriptionElem struct {
	XMLName   xmlName `xml:"create-subscription"`
	XMLNS     string  `xml:"xmlns,attr"`
	Stream    string  `xml:"stream,omitempty"`
	Filter    string  `xml:"filter,omitempty"`
	StartTime string  `xml:"startTime,omitempty"`
	StopTime  string  `xml:"stopTime,omitempty"`
}

// xmlName is a marker for encoding/xml so the create-subscription element
// nests properly inside the surrounding <rpc>.
type xmlName = struct {
	Local string `xml:"-"`
}

// CreateSubscription sends an RFC 5277 <create-subscription> RPC and
// returns the server's <ok/> response. Subsequent unsolicited
// <notification> frames are delivered via the channel returned by
// Notifications().
//
// stream selects which event stream to subscribe to ("NETCONF" is the
// always-available default). filter, startTime and stopTime are
// optional and may be empty.
//
// The standard create-subscription element lives in the
// urn:ietf:params:xml:ns:netconf:notification:1.0 namespace; this is
// distinct from the YANG-Push establish-subscription helper in
// subscription.go.
func (d *Driver) CreateSubscription(
	stream, filter, startTime, stopTime string,
	opts ...util.Option,
) (*response.NetconfResponse, error) {
	if stream == "" {
		stream = "NETCONF"
	}
	elem := &createSubscriptionElem{
		XMLNS:     "urn:ietf:params:xml:ns:netconf:notification:1.0",
		Stream:    stream,
		Filter:    filter,
		StartTime: startTime,
		StopTime:  stopTime,
	}

	m := d.buildPayload(elem)

	op := &OperationOptions{}
	for _, o := range opts {
		if err := o(op); err != nil {
			return nil, fmt.Errorf("scrapligo: create-subscription option: %w", err)
		}
	}

	r, err := d.sendRPC(m, op)
	if err != nil {
		return nil, err
	}
	if r.Failed != nil {
		return r, r.Failed
	}
	return r, nil
}
