pub trait ApiMessage {}

pub trait Header : ApiMessage {}

pub trait Request : ApiMessage {}

pub trait Response : ApiMessage {}

pub trait Data : ApiMessage {}
