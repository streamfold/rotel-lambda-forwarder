use lambda_runtime::Context;

#[derive(Clone)]
pub struct AwsAttributes {
    pub region: String,
    pub account_id: String,
    pub invoked_function_arn: String,
}

impl AwsAttributes {
    pub fn new(context: &Context) -> Self {
        let region = context
            .invoked_function_arn
            .split(':')
            .nth(3)
            .unwrap_or("unknown")
            .to_string();
        let account_id = context
            .invoked_function_arn
            .split(':')
            .nth(4)
            .unwrap_or("unknown")
            .to_string();
        Self {
            region,
            account_id,
            invoked_function_arn: context.invoked_function_arn.clone(),
        }
    }
}

#[cfg(test)]
impl Default for AwsAttributes {
    fn default() -> Self {
        Self {
            region: "us-east-1".to_string(),
            account_id: "912345678149".to_string(),
            invoked_function_arn: "arn:aws:lambda:us-east-1:912345678149:function:my-function"
                .to_string(),
        }
    }
}
